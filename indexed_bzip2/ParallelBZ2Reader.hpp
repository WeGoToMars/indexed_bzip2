#pragma once

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <limits>
#include <list>
#include <map>
#include <numeric>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

#include "bzip2.hpp"
#include "BZ2Reader.hpp"
#include "BitStringFinder.hpp"
#include "BlockDatabase.hpp"
#include "Cache.hpp"
#include "common.hpp"
#include "ParallelBitStringFinder.hpp"
#include "Prefetcher.hpp"
#include "ThreadPool.hpp"
#include "ThreadSafeQueue.hpp"


/**
 * Stores results in the order they are pushed and also stores a flag signaling that nothing will be pushed anymore.
 * The blockfinder will push block offsets and other actors, e.g., the prefetcher, may wait for and read the offsets.
 * Results will never be deleted, so you can assume the size to only grow.
 */
template<typename Value>
class StreamedResults
{
public:
    /**
     * std::vector would work as well but the reallocations during appending might slow things down.
     * For the index access operations, a container with random access iterator, would yield better performance.
     */
    using Values = std::deque<Value>;

    class ResultsView
    {
    public:
        ResultsView( const Values* results,
                     std::mutex*   mutex ) :
            m_results( results ),
            m_lock( *mutex )
        {
            if ( m_results == nullptr ) {
                throw std::invalid_argument( "Arguments may not be nullptr!" );
            }
        }

        [[nodiscard]] const Values&
        results() const
        {
            return *m_results;
        }

    private:
        Values const * const m_results;
        std::scoped_lock<std::mutex> const m_lock;
    };

public:
    [[nodiscard]] size_t
    size() const
    {
        std::scoped_lock lock( m_mutex );
        return m_results.size();
    }

    /**
     * @param timeoutInSeconds Use infinity or 0 to wait forever or not wait at all.
     * @return the result at the requested position.
     */
    [[nodiscard]] std::optional<Value>
    get( size_t position,
         double timeoutInSeconds = std::numeric_limits<double>::infinity() ) const
    {
        std::unique_lock lock( m_mutex );

        if ( timeoutInSeconds > 0 ) {
            const auto predicate = [&] () { return m_finalized || ( position < m_results.size() ); };

            if ( std::isfinite( timeoutInSeconds ) ) {
                const auto timeout = std::chrono::nanoseconds( static_cast<size_t>( timeoutInSeconds * 1e9 ) );
                m_changed.wait_for( lock, timeout, predicate );
            } else {
                m_changed.wait( lock, predicate );
            }
        }

        if ( position < m_results.size() ) {
            return m_results[position];
        }
        return std::nullopt;
    }

    void
    push( Value value )
    {
        std::scoped_lock lock( m_mutex );
        m_results.emplace_back( std::move( value ) );
        m_changed.notify_all();
    }

    void
    finalize()
    {
        std::scoped_lock lock( m_mutex );
        m_finalized = true;
        m_changed.notify_all();
    }

    [[nodiscard]] bool
    finalized() const
    {
        return m_finalized;
    }


    /** @return a view to the results, which also locks access to it using RAII. */
    [[nodiscard]] ResultsView
    results() const
    {
        return ResultsView( &m_results, &m_mutex );
    }


private:
    mutable std::mutex m_mutex;
    mutable std::condition_variable m_changed;

    Values m_results;
    std::atomic<bool> m_finalized = false;
};


/**
 * Will asynchronously find the next n block offsets after the last highest requested one.
 * It might find false positives and it won't find EOS blocks, so there is some post-processing necessary.
 */
class BlockFinder
{
public:
    explicit
    BlockFinder( int    fileDescriptor,
                 size_t parallelisation ) :
        m_bitStringFinder( fileDescriptor, bzip2::MAGIC_BITS_BLOCK )
    {}

    explicit
    BlockFinder( char const* buffer,
                 size_t      size,
                 size_t      parallelisation ) :
        m_bitStringFinder( buffer, size, bzip2::MAGIC_BITS_BLOCK )
    {}

    explicit
    BlockFinder( std::string const& filePath,
                 size_t             parallelisation ) :
        m_bitStringFinder( filePath, bzip2::MAGIC_BITS_BLOCK )
    {}

    ~BlockFinder()
    {
        m_cancelThread = true;

        if constexpr ( m_debugOutput ) {
            std::cerr << ( ThreadSafeOutput() << "[Block Finder] Destructor!" ).str();
        }

        std::scoped_lock lock( m_mutex );

        if constexpr ( m_debugOutput ) {
            std::cerr << ( ThreadSafeOutput() << "[Block Finder] Cancel thread!" ).str();
        }

        m_changed.notify_all();
    }

public:
    [[nodiscard]] size_t
    size() const
    {
        return m_blockOffsets.size();
    }

    [[nodiscard]] bool
    finalized() const
    {
        return m_blockOffsets.finalized();
    }

    /**
     * This call will track the requested block so that the finder loop will look up to that block.
     * Per default, with the infinite timeout, either a result can be returned or if not it means
     * we are finalized and the requested block is out of range!
     */
    [[nodiscard]] std::optional<size_t>
    get( size_t blockNumber,
         double timeoutInSeconds = std::numeric_limits<double>::infinity() )
    {
        {
            std::scoped_lock lock( m_mutex );
            m_highestRequestedBlockNumber = std::max( m_highestRequestedBlockNumber, blockNumber );
            m_changed.notify_all();
        }

        return m_blockOffsets.get( blockNumber, timeoutInSeconds );
    }

    /** @return Index for the block at the requested offset. */
    [[nodiscard]] size_t
    find( size_t encodedBlockOffsetInBits ) const
    {
        std::scoped_lock lock( m_mutex );

        /* m_blockOffsets is effectively double-locked but that's the price of abstraction. */
        const auto lockedOffsets = m_blockOffsets.results();
        const auto& blockOffsets = lockedOffsets.results();

        /* Find in sorted vector by bisection. */
        const auto match = std::lower_bound( blockOffsets.begin(), blockOffsets.end(), encodedBlockOffsetInBits );
        if ( ( match == blockOffsets.end() ) || ( *match != encodedBlockOffsetInBits ) ) {
            throw std::out_of_range( "No block with the specified offset exists in the block map!" );
        }

        return std::distance( blockOffsets.begin(), match );
    }

private:
    void
    blockFinderMain()
    {
        if constexpr ( m_debugOutput ) {
            std::cerr << ( ThreadSafeOutput() << "[Block Finder] Boot" ).str();
        }

        while ( !m_cancelThread ) {
            std::unique_lock lock( m_mutex );
            /* m_blockOffsets.size() will only grow, so we don't need to be notified when it changes! */
            m_changed.wait( lock, [this]{
                return m_cancelThread || ( m_blockOffsets.size() <= m_highestRequestedBlockNumber + m_prefetchCount );
            } );
            if ( m_cancelThread ) {
                break;
            }

            /* Assuming a valid BZ2 file, the time for this find method should be bounded and
             * responsive enough when reacting to cancelations. */
            const auto blockOffset = m_bitStringFinder.find();
            if ( blockOffset == std::numeric_limits<size_t>::max() ) {
                break;
            }

            m_blockOffsets.push( blockOffset );

            lock.unlock(); // Unlock for a little while so that others can acquire the lock!
        }

        m_blockOffsets.finalize();

        if ( m_debugOutput ) {
            std::cerr << ( ThreadSafeOutput() << "[Block Finder] Found" << m_blockOffsets.size() << "blocks" ).str();
        }
    }

private:
    mutable std::mutex m_mutex; /**< Only variables accessed by the asynchronous main loop need to be locked. */
    std::condition_variable m_changed;

    StreamedResults<size_t> m_blockOffsets;

    size_t m_highestRequestedBlockNumber{ 0 };

    /**
     * Only hardware_concurrency slows down decoding! I guess because in the worst case all decoding
     * threads finish at the same time and now the bit string finder would need to find n new blocks
     * in the time it takes to decode one block! In general, the higher this number, the higher the
     * longer will be the initial CPU utilization.
     */
    const size_t m_prefetchCount = 3 * std::thread::hardware_concurrency();

    /** @todo Using ParallelBitStringFinder leads to tests failing when trying to read the whole file.
     *        It seems like the last block before EOF gets dropped by the read call somehow! */
    BitStringFinder<bzip2::MAGIC_BITS_SIZE> m_bitStringFinder;
    std::atomic<bool> m_cancelThread{ false };
    static constexpr bool m_debugOutput = true;

    JoiningThread m_blockFinder{ &BlockFinder::blockFinderMain, this };
};


/**
 * Should get block offsets and decoded sizes and will do conversions between decoded and encoded offsets!
 * The idea is that at first any forward seeking should be done using read calls and the read call will
 * push all block information to the BlockMapBuilder. And because ParallelBZ2Reader should not be called from
 * differen threads, there should never be a case that lookups to this function should have to wait for
 * other threads to push data into us!
 * This is used by the worker threads, so it must be thread-safe!
 */
class BlockMap
{
public:
    struct BlockInfo
    {
    public:
        [[nodiscard]] bool
        contains( size_t dataOffset ) const
        {
            return ( decodedOffsetInBytes <= dataOffset ) && ( dataOffset < decodedOffsetInBytes + decodedSizeInBytes );
        }

    public:
        /**< each BZ2 block in the stream will be given an increasing index number. */
        size_t blockIndex{ 0 };
        size_t encodedOffsetInBits{ 0 };
        size_t encodedSizeInBits{ 0 };
        size_t decodedOffsetInBytes{ 0 };
        size_t decodedSizeInBytes{ 0 };
    };

public:
    /** BlockFinder is used to determine which blocks exist and which are still missing! */
    BlockMap( std::map<size_t, size_t>* blockToDataOffsets ) :
        m_blockToDataOffsets( blockToDataOffsets )
    {
        if ( m_blockToDataOffsets == nullptr ) {
            throw std::invalid_argument( "May not give invalid pointers as arguments!" );
        }
    }


    void
    insert( size_t encodedBlockOffset,
            size_t encodedSize,
            size_t decodedSize )
    {
        std::scoped_lock lock( m_mutex );

        std::optional<size_t> decodedOffset;
        if ( m_blockToDataOffsets->empty() ) {
            decodedOffset = 0;
        } else if ( encodedBlockOffset > m_blockToDataOffsets->rbegin()->first ) {
            decodedOffset = m_blockToDataOffsets->rbegin()->second + m_lastBlockDecodedSize;
        }

        /* If successive value or empty, then simply append */
        if ( decodedOffset ) {
            m_blockToDataOffsets->emplace( encodedBlockOffset, *decodedOffset );
            m_lastBlockDecodedSize = decodedSize;
            m_lastBlockEncodedSize = encodedSize;
            return;
        }

        /* Generally, block inserted offsets should always be increasing!
         * But do ignore duplicates after confirming that there is no data inconsistency. */
        const auto match = m_blockToDataOffsets->find( encodedBlockOffset );

        if ( match == m_blockToDataOffsets->end() ) {
            throw std::invalid_argument( "Inserted block offsets should be strictly increasing!" );
        }

        if ( std::next( match ) == m_blockToDataOffsets->end() ) {
            throw std::logic_error( "This case should already be handled with the first if clause!" );
        }

        const auto impliedDecodedSize = std::next( match )->second - match->second;
        if ( impliedDecodedSize != decodedSize ) {
            throw std::invalid_argument( "Got duplicate block offset with inconsistent size!" );
        }

        /* Quietly ignore duplicate insertions. */
    }

    /**
     * Returns the block containing the given data offset. May return a block which does not contain the given
     * offset. In that case it will be the last block.
     */
    [[nodiscard]] BlockInfo
    findDataOffset( size_t dataOffset ) const
    {
        std::scoped_lock lock( m_mutex );

        BlockInfo result;

        /* find offset from map (key and values should sorted, so we can bisect!) */
        /** @todo because map iterators are not random access iterators and because the comparison operator
         * is not very time-consuming, it's probably still effectively linear complexity. */
        const auto blockOffset = std::lower_bound(
            m_blockToDataOffsets->rbegin(), m_blockToDataOffsets->rend(), std::make_pair( 0, dataOffset ),
            [] ( std::pair<size_t, size_t> a, std::pair<size_t, size_t> b ) { return a.second > b.second; } );

        if ( blockOffset == m_blockToDataOffsets->rend() ) {
            return result;
        }

        if ( dataOffset < blockOffset->second ) {
            throw std::logic_error( "Algorithm for finding the block to an offset is faulty!" );
        }

        result.encodedOffsetInBits = blockOffset->first;
        result.decodedOffsetInBytes = blockOffset->second;
        /** @todo O(n) */
        result.blockIndex = std::distance( blockOffset, m_blockToDataOffsets->rend() ) - 1;

        if ( blockOffset == m_blockToDataOffsets->rbegin() ) {
            result.decodedSizeInBytes = m_lastBlockDecodedSize;
            result.encodedSizeInBits = m_lastBlockEncodedSize;
        } else {
            const auto higherBlock = std::prev( /* reverse! */ blockOffset );
            if ( higherBlock->second < blockOffset->second ) {
                std::logic_error( "Data offsets are not monotonically increasing!" );
            }
            result.decodedSizeInBytes = higherBlock->second - blockOffset->second;
            result.encodedSizeInBits = higherBlock->first - blockOffset->first;
        }

        return result;
    }


    [[nodiscard]] size_t
    blockCount() const
    {
        std::scoped_lock lock( m_mutex );
        return m_blockToDataOffsets->size();
    }

private:
    /** If complete, the last block will be of size 0 and indicate the end of stream! */
    std::map<size_t, size_t>* const m_blockToDataOffsets;

    size_t m_lastBlockEncodedSize{ 0 }; /**< Encoded block size of m_blockToDataOffsets.rbegin() */
    size_t m_lastBlockDecodedSize{ 0 }; /**< Decoded block size of m_blockToDataOffsets.rbegin() */

    size_t m_highestDataOffset{ 0 }; /**< used only for sanity check. */

    mutable std::mutex m_mutex;
};



/**
 * Manages block data access. Calls to members are not thread-safe!
 * Requested blocks are cached and accesses may trigger prefetches,
 * which will be fetched in parallel using a thread pool.
 */
template<typename FetchingStrategy = FetchingStrategy::FetchNext>
class BlockFetcher
{
public:
    struct BlockHeaderData
    {
        size_t encodedOffsetInBits{ std::numeric_limits<size_t>::max() };
        size_t encodedSizeInBits{ 0 }; /**< When calling readBlockheader, only contains valid data if EOS block. */

        uint32_t expectedCRC{ 0 };  /**< if isEndOfStreamBlock == true, then this is the stream CRC. */
        bool isEndOfStreamBlock{ false };
        bool isEndOfFile{ false };
    };

    struct BlockData :
        public BlockHeaderData
    {
        std::vector<uint8_t> data;
        uint32_t calculatedCRC{ 0xFFFFFFFFL };
    };

public:
    /** @todo might also need BlockMap in order to have a countable index to use for PrefetchStrategy! */
    BlockFetcher( BitReader                    bitReader,
                  std::shared_ptr<BlockFinder> blockFinder,
                  size_t                       parallelism ) :
        m_bitReader    ( bitReader ),
        m_blockFinder  ( std::move( blockFinder ) ),
        m_blockSize100k( bzip2::readBzip2Header( bitReader ) ),
        m_parallelism  ( parallelism == 0 ? std::thread::hardware_concurrency() : parallelism ),
        m_cache        ( 16 + m_parallelism ),
        m_threadPool   ( m_parallelism )
    {}

    ~BlockFetcher()
    {
        m_cancelThreads = true;
        m_cancelThreadsCondition.notify_all();
    }

    /**
     * Fetches, prefetches, caches, and returns result.
     */
    [[nodiscard]] std::shared_ptr<BlockData>
    get( size_t blockOffset,
         size_t blockIndex = std::numeric_limits<size_t>::max() )
    {
        /* First, access cache before data might get evicted! (May return std::nullopt) */
        auto result = m_cache.get( blockOffset );

        if ( blockIndex == std::numeric_limits<size_t>::max() ) {
            blockIndex = m_blockFinder->find( blockOffset );
        }


        /* Check whether the desired offset is prefetched. */
        std::future<BlockData> resultFuture;
        if ( !result ) {
            const auto match = std::find_if(
                m_prefetching.begin(), m_prefetching.end(),
                [blockOffset] ( auto& kv ){ return kv.first == blockOffset; }
            );

            if ( match != m_prefetching.end() ) {
                resultFuture = std::move( match->second );
                m_prefetching.erase( match );
                assert( resultFuture.valid() );
            }
        }

        /* Start requested calculation if necessary. */
        if ( !result && !resultFuture.valid() ) {
            resultFuture = m_threadPool.submitTask( [this, blockOffset](){ return decodeBlock( blockOffset ); } );
            assert( resultFuture.valid() );
        }

        /* Check for ready prefetches and move them to cache. */
        for ( auto it = m_prefetching.begin(); it != m_prefetching.end(); ) {
            auto& [prefetchedBlockOffset, prefetchedFuture] = *it;

            using namespace std::chrono;
            if ( prefetchedFuture.valid() && ( prefetchedFuture.wait_for( 0s ) == std::future_status::ready ) ) {
                m_cache.insert( prefetchedBlockOffset, std::make_shared<BlockData>( prefetchedFuture.get() ) );

                it = m_prefetching.erase( it );
            } else {
                ++it;
            }
        }

        /* Get blocks to prefetch. In order to avoid oscillating caches, the fetching strategy should ony return
         * less than the cache size number of blocks. It is fine if that means no work is being done in the background
         * for some calls to 'get'! */
        m_fetchingStrategy.fetch( blockIndex );
        auto blocksToPrefetch = m_fetchingStrategy.prefetch();
        for ( auto blockIndexToPrefetch : blocksToPrefetch ) {
            if ( m_prefetching.size() + /* thread with the requested block */ 1 >= m_parallelism ) {
                break;
            }

            if ( blockIndexToPrefetch == blockIndex ) {
                throw std::logic_error( "The fetching strategy should not return the "
                                        "last fetched block for prefetching!" );
            }

            /* Do not prefetch already cached/prefetched blocks or block indexes which are not yet in the block map. */
            const auto prefetchBlockOffset = m_blockFinder->get( blockIndexToPrefetch, 0 );
            if ( auto match = m_prefetching.find( *prefetchBlockOffset );
                 (match != m_prefetching.end() )
                 || !prefetchBlockOffset.has_value()
                 || m_cache.get( prefetchBlockOffset.value() ).has_value() )
            {
                continue;
            }

            auto decodeTask = [this, offset = *prefetchBlockOffset](){ return decodeBlock( offset ); };
            auto prefetchedFuture = m_threadPool.submitTask( std::move( decodeTask ) );
            const auto [_, wasInserted] = m_prefetching.emplace( *prefetchBlockOffset, std::move( prefetchedFuture ) );
            if ( !wasInserted ) {
                std::logic_error( "Submitted future could not be inserted to prefetch queue!" );
            }
        }

        if ( m_threadPool.unprocessedTasksCount() > m_parallelism ) {
            throw std::logic_error( "The thread pool should not have more tasks than there are prefetching futures!" );

        }

        /* Return result */
        if ( result ) {
            assert( !resultFuture.valid() );
            return *result;
        }

        result = std::make_shared<BlockData>( resultFuture.get() );
        m_cache.insert( blockOffset, *result );
        return *result;
    }

    [[nodiscard]] BlockHeaderData
    readBlockHeader( size_t blockOffset ) const
    {
        BitReader bitReader( m_bitReader );
        bitReader.seek( blockOffset );
        bzip2::Block block( std::move( bitReader ) );

        BlockHeaderData result;
        result.encodedOffsetInBits = blockOffset;
        result.isEndOfStreamBlock = block.eos();
        result.isEndOfFile = block.eof();
        result.expectedCRC = block.bwdata.headerCRC;

        if ( block.eos() ) {
            std::cerr << ( ThreadSafeOutput() << "EOS block at" << blockOffset ).str();
            result.encodedSizeInBits = block.encodedSizeInBits;
        }

        return result;
    }

private:
    [[nodiscard]] BlockData
    decodeBlock( size_t blockOffset ) const
    {
        BitReader bitReader( m_bitReader );
        bitReader.seek( blockOffset );
        bzip2::Block block( std::move( bitReader ) );

        BlockData result;
        result.encodedOffsetInBits = blockOffset;
        result.isEndOfStreamBlock = block.eos();
        result.isEndOfFile = block.eof();
        result.expectedCRC = block.bwdata.headerCRC;

        if ( block.eos() ) {
            std::cerr << ( ThreadSafeOutput() << "EOS block at" << blockOffset ).str();
            result.encodedSizeInBits = block.encodedSizeInBits;
            return result;
        }

        block.readBlockData();

        size_t decodedDataSize = 0;
        do
        {
            /* Increase buffer for next batch. Unfortunately we can't find the perfect size beforehand because
             * we don't know the amount of decoded bytes in the block. */
            if ( result.data.empty() ) {
                /* Just a guess to avoid reallocations at smaller sizes. Must be >= 255 though because the decodeBlock
                 * method might return up to 255 copies caused by the runtime length decoding! */
                result.data.resize( m_blockSize100k * 100'000 + 255 );
            } else {
                result.data.resize( result.data.size() * 2 );
            }

            decodedDataSize += block.bwdata.decodeBlock( result.data.size() - 255 - decodedDataSize,
                                                         reinterpret_cast<char*>( result.data.data() ) + decodedDataSize );
        }
        while ( block.bwdata.writeCount > 0 );

        result.data.resize( decodedDataSize );
        result.encodedSizeInBits = block.encodedSizeInBits;
        result.calculatedCRC = block.bwdata.dataCRC;

        return result;
    }

private:
    /* Variables required by decodeBlock and which therefore should be either const or locked. */
    const BitReader m_bitReader;
    const std::shared_ptr<BlockFinder> m_blockFinder;
    uint8_t m_blockSize100k;

    /** Future holding the number of found magic bytes. Used to determine whether the thread is still running. */
    std::atomic<bool> m_cancelThreads{ false };
    std::condition_variable m_cancelThreadsCondition;

    const size_t m_parallelism;

    Cache</** block offset in bits */ size_t, std::shared_ptr<BlockData> > m_cache;
    FetchingStrategy m_fetchingStrategy;

    std::map<size_t, std::future<BlockData> > m_prefetching;
    ThreadPool m_threadPool;
};


/**
 * @note Calls to this class are not thread-safe! Even though they use threads to evaluate them in parallel.
 */
class ParallelBZ2Reader :
    public BZ2Reader
{
public:
    using BlockFetcher = ::BlockFetcher<FetchingStrategy::FetchNext>;

public:
    explicit
    ParallelBZ2Reader( int fileDescriptor ) :
        BZ2Reader( fileDescriptor ),
        m_blockFinder( std::make_shared<BlockFinder>( fileDescriptor, m_parallelisation ) )
    {}

    ParallelBZ2Reader( const char*  bz2Data,
                       const size_t size ) :
        BZ2Reader( bz2Data, size ),
        m_blockFinder( std::make_shared<BlockFinder>( bz2Data, size, m_parallelisation ) )
    {}

    ParallelBZ2Reader( const std::string& filePath ) :
        BZ2Reader( filePath ),
        m_blockFinder( std::make_shared<BlockFinder>( filePath, m_parallelisation ) )
    {}

public:
    long int
    read( const int    outputFileDescriptor = -1,
          char* const  outputBuffer = nullptr,
          const size_t nBytesToRead = std::numeric_limits<size_t>::max() )
    {
        if ( eof() || ( nBytesToRead == 0 ) ) {
            return 0;
        }

        if ( !m_blockFetcher ) {
            m_blockFetcher = std::make_unique<BlockFetcher>( m_bitReader, m_blockFinder, m_parallelisation );
        }

        if ( !m_blockFetcher ) {
            throw std::logic_error( "Block fetcher should have been initialized." );
        }

        /** @todo read Bzip2 header for CRC check and stuff. Might be problematic for multiple Bzip2 streams. */
        /** @todo Need to fill BZ2Reader::m_blockToDataOffsets with data from decoded block sizes */

        size_t nBytesDecoded = 0;
        while ( ( nBytesDecoded < nBytesToRead ) && !eof() ) {
            std::shared_ptr<BlockFetcher::BlockData> blockData;

            const auto blockInfo = m_blockMap->findDataOffset( m_currentPosition );
            if ( !blockInfo.contains( m_currentPosition ) ) {
                /* Fetch new block for the first time and add information to block map. */
                const auto blockIndex = m_blockMap->blockCount();
                const auto encodedOffsetInBits = m_blockFinder->get( blockIndex );
                if ( !encodedOffsetInBits ) {
                    std::cerr << ( ThreadSafeOutput() << "EOF reached 2" ).str();
                    m_blockToDataOffsetsComplete = true;
                    m_atEndOfFile = true;
                    break;
                }


                blockData = m_blockFetcher->get( *encodedOffsetInBits, blockIndex );
                m_blockMap->insert( blockData->encodedOffsetInBits, blockData->encodedSizeInBits, blockData->data.size() );

                /** @todo Try to read next block here in order to insert EOS blocks, which the block finder ignores! */
                /* Check whether the next block is an EOS block, which has a different magic byte string
                 * and therefore will not be found by the block finder! Such a block will span 48 + 32 + (0..7) bits.
                 * However, the last 0 to 7 bits are only padding and not needed! */
                if ( !blockData->isEndOfFile ) {
                    const auto nextBlockHeaderData = m_blockFetcher->readBlockHeader( blockData->encodedOffsetInBits +
                                                                                      blockData->encodedSizeInBits );
                    if ( nextBlockHeaderData.isEndOfStreamBlock ) {
                        m_blockMap->insert( nextBlockHeaderData.encodedOffsetInBits,
                                            nextBlockHeaderData.encodedSizeInBits,
                                            0 );
                    }
                }
                continue;
            }

            const auto offsetInBlock = m_currentPosition - blockInfo.decodedOffsetInBytes;
            blockData = m_blockFetcher->get( blockInfo.encodedOffsetInBits );

            if ( offsetInBlock >= blockData->data.size() ) {
                throw std::logic_error( "Block does not contain the requested offset even though it "
                                        "shouldn't be according to block map!" );
            }

            const auto nBytesToDecode = std::min( blockData->data.size() - offsetInBlock,
                                                  nBytesToRead - nBytesDecoded );
            nBytesDecoded += writeResult( outputFileDescriptor,
                                          outputBuffer == nullptr ? nullptr : outputBuffer + nBytesDecoded,
                                          reinterpret_cast<const char*>( blockData->data.data() + offsetInBlock ),
                                          nBytesToDecode );
            m_currentPosition += nBytesToDecode;
        }

        return nBytesDecoded;
    }

    size_t
    seek( long long int offset,
          int           origin = SEEK_SET ) override
    {
        switch ( origin )
        {
        case SEEK_CUR:
            offset = tell() + offset;
            break;
        case SEEK_SET:
            break;
        case SEEK_END:
            /* size() requires the block offsets to be available! */
            if ( !m_blockToDataOffsetsComplete ) {
                read();
            }
            offset = size() + offset;
            break;
        }

        offset = std::max<decltype( offset )>( 0, offset );

        if ( static_cast<size_t>( offset ) == tell() ) {
            return offset;
        }

        /* Backward seeking is no problem at all! 'tell' may only return <= size()
         * as value meaning we are now < size() and therefore EOF can be cleared! */
        if ( static_cast<size_t>( offset ) < tell() ) {
            m_atEndOfFile = false;
            m_currentPosition = offset;
            return offset;
        }

        /* m_blockMap is only accessed by read and seek, which are not to be called from different threads,
         * so we do not have to lock it. */
        const auto blockInfo = m_blockMap->findDataOffset( offset );
        if ( static_cast<size_t>( offset ) < blockInfo.decodedOffsetInBytes ) {
            throw std::logic_error( "Block map returned unwanted block!" );
        }

        if ( blockInfo.contains( offset ) ) {
            m_atEndOfFile = false;
            m_currentPosition = offset;
            return tell();
        }

        /** @todo Move m_blockToDataOffsetsComplete into BlockMap and lock everything in this seek function? */
        assert( static_cast<size_t>( offset ) - blockInfo.decodedOffsetInBytes > blockInfo.decodedSizeInBytes );
        if ( m_blockToDataOffsetsComplete ) {
        #if 0
            m_currentPosition = offset;
            return tell();
        #endif
            m_atEndOfFile = true;
            m_currentPosition = size();
            return tell();
        }

        /* Jump to furthest known point as performance optimization. Note that even if that is right after
         * the last byte, i.e., offset == size(), then no eofbit is set even in ifstream! In ifstream you
         * can even seek to after the file end with no fail bits being set in my tests! */
        m_atEndOfFile = false;
        m_currentPosition = blockInfo.decodedOffsetInBytes + blockInfo.decodedSizeInBytes;
        read( -1, nullptr, offset - tell() );
        return tell();
    }

private:
    size_t
    writeResult( int         const outputFileDescriptor,
                 char*       const outputBuffer,
                 char const* const dataToWrite,
                 size_t      const dataToWriteSize )
    {
        size_t nBytesFlushed = dataToWriteSize; // default then there is neither output buffer nor file device given

        if ( outputFileDescriptor >= 0 ) {
            const auto nBytesWritten = write( outputFileDescriptor, dataToWrite, dataToWriteSize );
            nBytesFlushed = std::max<decltype( nBytesWritten )>( 0, nBytesWritten );
        }

        if ( outputBuffer != nullptr ) {
            std::memcpy( outputBuffer, dataToWrite, nBytesFlushed );
        }

        return nBytesFlushed;
    }

private:
    /** Necessary for prefetching decoded blocks in parallel. */
    const unsigned int m_parallelisation{ std::thread::hardware_concurrency() };
    const std::shared_ptr<BlockFinder> m_blockFinder;
    const std::shared_ptr<BlockMap> m_blockMap{ std::make_unique<BlockMap>( &m_blockToDataOffsets ) };
    std::unique_ptr<BlockFetcher> m_blockFetcher;
};
