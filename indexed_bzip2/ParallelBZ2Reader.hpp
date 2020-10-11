#pragma once

#include <algorithm>
#include <cstddef>
#include <limits>
#include <list>
#include <map>
#include <string>
#include <thread>
#include <utility>

#include "bzip2.hpp"
#include "BZ2Reader.hpp"
#include "BitStringFinder.hpp"
#include "BlockDatabase.hpp"
#include "ThreadPool.hpp"
#include "ThreadSafeQueue.hpp"


/**
 * The idea is to use where possible the original BZ2Reader functions but extend them for parallelism.
 * Each worker thread has its own BitReader object in order to be able to access the input independently.
 * The states required for the tasks submitted to the thread pool to be evaluable are held by this class.
 *
 * Idea:
 *   Master Thread:
 *      This is the main thread, which normally is outside this class. Whenever this class' methods are
 *      are called it evaluates them and might distribute work to the thread pool.
 *   Block Finder Thread:
 *     Goes sequentially over file to find all magic byte occurences and stores their offsets.
 *     They might be false positives, so exceptions thrown when trying to decode have to be handled.
 *     Thread safety considerations:
 *       - [ ] The Master Thread is already non-busily waiting for changes in the container.
 *             Therefore it can't monitor another condition signaling when the Block Finder Thread has finished.
 *             For that reason, the last offset the Block Finder should queue is std::numeric_limits<size_t>::max().
 *             If the Block Finder can't write that last value there should be a fallback in form of a wait timeout
 *             and check whether the Block Finder is still running. A timeout of 1s should be no problem because it
 *             should only happen very rarely and only on errors.
 *             To detect whether a thread is running in an exception-safe manner, it's best to use a future.
 *     Performance considerations:
 *       - [ ] It should not try to decode the file in one go to improve latency with the other decoding threads.
 *             Only decode a safety buffer, e.g., as many magic bytes as twice the thread pool.
 *             More than one thread pool of work to account for fast processed work caused by false positives.
 *       - [ ] It should be cancelable. Don't read the whole 100+GB file if the user just wanted the first bytes.
 *             This is kinda already in the first point, however, it has to be considered specially for the case
 *             the file contains no magic bytes at all!
 *   Thread Pool Decoders:
 *      These get work distributed by this class on some events (at least read) and try to decode it
 *      to an internal buffer. The work is distributed serially, however, locking is still required,
 *      so that pointers and references to vector elements don't just suddenly get lost because the
 *      Block Finder Thread triggered a reallocation.
 *
 * @note Calls to this class are not thread-safe! Even though they use threads to evaluate them in parallel.
 */
class ParallelBZ2Reader :
    public BZ2Reader
{
public:
    /** @todo Add parallelism parameter. */
    template<class... T_Args>
    explicit
    ParallelBZ2Reader( T_Args&&... args ) :
        BZ2Reader( std::forward<T_Args>( args )... ),
        m_blockFinder( &ParallelBZ2Reader::blockFinderMain, this ),
        m_threadPool( std::thread::hardware_concurrency() ),
        m_workDispatcher( &ParallelBZ2Reader::workDispatcherMain, this )
    {}

    ~ParallelBZ2Reader()
    {
        m_cancelThreads = true;
        m_cancelThreadsCondition.notify_all();
    }

public:
    int
    read( const int    outputFileDescriptor = -1,
          char* const  outputBuffer = nullptr,
          const size_t nBytesToRead = std::numeric_limits<size_t>::max() )
    {
        if ( eof() || ( nBytesToRead == 0 ) ) {
            return 0;
        }

        /* try to flush remnants in output buffer from interrupted last call */
        size_t nBytesDecoded = flushOutputBuffer( outputFileDescriptor, outputBuffer, nBytesToRead );

        while ( ( nBytesDecoded < nBytesToRead ) && !eof() ) {
            /** @todo CLEAN UP this mess. I just wanted some results for now ... */
            const uint8_t* buffer = nullptr;
            size_t size = 0;
            while ( buffer == nullptr ) {
                const auto wasComplete = m_blocks.completed();
                const auto [buffer2, size2] = m_blocks.data( m_currentPosition );

                //if ( ( buffer2 == nullptr ) && !wasComplete ) {
                //    std::cerr << "buffer null but database not complete!\n";
                //}
                if ( ( buffer2 == nullptr ) && wasComplete ) {
                    m_atEndOfFile = true;
                    return 0;
                }
                buffer = buffer2;
                size = size2;
                if ( buffer == nullptr ) {
                    using namespace std::chrono_literals;
                    std::this_thread::sleep_for( 100ms );
                }
            }

            const auto nBytesToDecode = std::min( size,
                                                  std::min( m_decodedBuffer.size(),
                                                            nBytesToRead - nBytesDecoded ) );
            for ( size_t i = 0; i < nBytesToDecode; ++i ) {
                m_decodedBuffer[i] = buffer[i];
            }
            m_decodedBufferPos = nBytesToDecode;

            nBytesDecoded += flushOutputBuffer( outputFileDescriptor,
                                                outputBuffer == nullptr ? nullptr : outputBuffer + nBytesDecoded,
                                                nBytesToRead - nBytesDecoded );
            m_currentPosition += nBytesToDecode;
        }

        return nBytesDecoded;
    }

    size_t
    seek( long long int offset,
          int           origin = SEEK_SET ) override
    {
        throw std::invalid_argument( "Seeking not implemented yet for the parallel decoder, use the serial one!" );
    }

    void
    setBlockOffsets( std::map<size_t, size_t> offsets )
    {
        throw std::invalid_argument( "Offset loading not implemented yet for the parallel decoder, use the serial one!" );
        m_blocks.setBlockOffsets( offsets );
        m_blocks.finalize();
    }

private:
    void
    blockFinderMain()
    {
        auto bitStringFinder =
            m_bitReader.fp() == nullptr
            ? BitStringFinder<1>( reinterpret_cast<const char*>( m_bitReader.buffer().data() ),
                                  m_bitReader.buffer().size(), { bzip2::MAGIC_BITS_BLOCK }, bzip2::MAGIC_BITS_SIZE )
            : BitStringFinder<1>( m_bitReader.fileno(), { bzip2::MAGIC_BITS_BLOCK }, bzip2::MAGIC_BITS_SIZE );

        /* Only hardware_concurrency slows down decoding! I guess because in the worst case all decoding
         * threads finish at the same time and now the bit string finder would need to find n new blocks
         * in the time it takes to decode one block! In general, the higher this number, the higher the
         * the memory usage. */
        const size_t maxBlocksToQueue = 3 * std::thread::hardware_concurrency();

        size_t bitOffset = std::numeric_limits<size_t>::max();
        while ( !m_cancelThreads ) {
            /* Only try to find a new offset if the old one has been inserted and therefore is uninitialized. */
            if ( bitOffset == std::numeric_limits<size_t>::max() ) {
                /** @todo extend find method to take a maximum number of bytes to
                 * analyse to check the condition_variable */
                bitOffset = bitStringFinder.find();
                if ( bitOffset == std::numeric_limits<size_t>::max() ) {
                    break;
                }
            }

            if ( m_blocks.unprocessedBlockCount() < maxBlocksToQueue ) {
                m_blocks.insertBlock( bitOffset );
                bitOffset = std::numeric_limits<size_t>::max();
                continue;
            }

            m_blocks.waitUntilChanged( 0.01 );
        }

        m_blocks.finalize();
        m_blockToDataOffsets = m_blocks.blockOffsets();
        m_blockToDataOffsetsComplete = true;
    }

    void
    workDispatcherMain()
    {
        std::list<decltype( m_threadPool.submitTask( std::function<void()>() ) )> futures;

        while ( !m_cancelThreads ) {
            /** @todo make this work after seeking or after setBlockOffsets in general! */
            if ( m_blocks.completed() )  {
                break;
            }
            size_t blockOffset;
            /** @todo use something better instead of catching! */
            try {
                blockOffset = m_blocks.takeBlockForProcessing();
            } catch ( const std::invalid_argument& e ) {
                break;
            }
            if ( blockOffset != std::numeric_limits<size_t>::max() ) {
                auto result = m_threadPool.submitTask( [this, blockOffset] () { decodeBlock( blockOffset ); } );
                futures.emplace_back( std::move( result ) );
            }

            /** @todo only decode up to hardware_concurrency blocks, then wait for old ones to be cleared! */
            if ( m_blocks.unprocessedBlockCount() == 0 ) {
                m_blocks.waitUntilChanged( 0.01 ); /* Every 100ms, check whether this thread has been canceled. */
            }
        }

        for ( auto& future : futures ) {
            future.get();
        }
    }

    void
    decodeBlock( size_t blockOffset )
    {
        BitReader bitReader( m_bitReader );
        bitReader.seek( blockOffset );
        bzip2::Block block( bitReader );

        if ( block.eos() ) {
            std::cerr << "[" << std::this_thread::get_id() << "] ";
            std::cerr << "EOS block at " << blockOffset << "\n";
            const auto encodedBitsCount = bitReader.tell() - blockOffset;
            m_blocks.setBlockData( blockOffset, encodedBitsCount, {}, 0, block.bwdata.headerCRC, block.eos() );
            return;
        }

        block.readBlockData();

        std::vector<uint8_t> decodedData;
        size_t decodedDataSize = 0;
        do
        {
            /* Increase buffer for next batch. Unfortunately we can't find the perfect size beforehand because
             * we don't know the amount of decoded bytes in the block. */
            if ( decodedData.empty() ) {
                /* Just a guess to avoid reallocations at smaller sizes. Must be >= 255 though because the decodeBlock
                 * method might return up to 255 copies caused by the runtime length decoding! */
                decodedData.resize( m_blockSize100k * 100'000 + 255 );
            } else {
                decodedData.resize( decodedData.size() * 2 );
            }

            decodedDataSize += block.bwdata.decodeBlock( decodedData.size() - 255 - decodedDataSize,
                                                         reinterpret_cast<char*>( decodedData.data() ) + decodedDataSize );
        }
        while ( block.bwdata.writeCount > 0 );
        decodedData.resize( decodedDataSize );

        const auto encodedBitsCount = bitReader.tell() - blockOffset;
        m_blocks.setBlockData( blockOffset, encodedBitsCount, std::move( decodedData ),
                               block.bwdata.dataCRC, block.bwdata.headerCRC, block.eos() );

        /** @todo try decoding the next block or maybe just insert the end offset into m_blocks if it does not
         *        exist already! Necessary because EOS blocks have different magic bytes. */

        //std::cerr << "Decoded block at " << blockOffset << " on thread " << std::this_thread::get_id()
        //          << ", which contained " << decodedDataSize << " bytes\n";
    }

private:
    BlockDatabase m_blocks;

    /** Future holding the number of found magic bytes. Used to determine whether the thread is still running. */
    std::atomic<bool> m_cancelThreads{ false };
    std::condition_variable m_cancelThreadsCondition;

    JoiningThread m_blockFinder;
    ThreadPool    m_threadPool;
    JoiningThread m_workDispatcher;
};
