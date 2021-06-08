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
#include "common.hpp"
#include "ParallelBitStringFinder.hpp"
#include "ThreadPool.hpp"
#include "ThreadSafeQueue.hpp"


#include <sstream>


class ConcurrencyBroker
{
public:
    void
    waitForChange( double timeoutSeconds = std::numeric_limits<double>::infinity() )
    {
        if ( std::isnan( timeoutSeconds ) || ( timeoutSeconds < 0. ) ) {
            throw std::invalid_argument( "Time must be a non-negative number!" );
        }

        std::unique_lock<std::mutex> lock( m_mutex );
        if ( timeoutSeconds == std::numeric_limits<double>::infinity() ) {
            m_changed.wait( lock );
        } else {
            const auto timeout = std::chrono::nanoseconds( static_cast<size_t>( timeoutSeconds * 1e9 ) );
            m_changed.wait_for( lock, timeout );
        }
    }

    std::mutex&
    mutex()
    {
        return m_mutex;
    }

    void
    notifyChange()
    {
        m_changed.notify_all();
    }

private:
    std::mutex m_mutex;
    std::condition_variable m_changed;
};


/**
 * Stores results in the order they are pushed and also stores a flag signaling that nothing will be pushed anymore.
 * The blockfinder will push block offsets and other actors, e.g., the prefetcher, may wait for and read the offsets.
 * Results will never be deleted, so you can assume the size to only grow.
 */
template<typename Value>
class StreamedResults
{
public:
    [[nodiscard]] size_t
    size() const
    {
        std::scoped_lock lock( m_mutex );
        return m_results.size();
    }

    /**
     * @return the result at the requested position. Will not wait if that result is not available yet.
     */
    [[nodiscard]] std::optional<Value>
    test( size_t position ) const
    {
        std::scoped_lock lock( m_mutex );
        if ( position < m_results.size() ) {
            return m_results[position];
        }
        return std::nullopt;
    }

    /**
     * @return the result at the requested position. Will only return
     *         if the result is available or if the class is finalized.
     */
    [[nodiscard]] std::optional<Value>
    get( size_t position ) const
    {
        std::unique_lock lock( m_mutex );
        m_changed.wait( lock, [&] () { return m_finalized || ( position < m_results.size() ); } );
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

private:
    mutable std::mutex m_mutex;
    mutable std::condition_variable m_changed;

    std::deque<Value> m_results;
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
    BlockFinder( int fileDescriptor ) :
        m_bitStringFinder( fileDescriptor, bzip2::MAGIC_BITS_BLOCK )
    {}

    explicit
    BlockFinder( const char* buffer,
                 size_t      size ) :
        m_bitStringFinder( buffer, size, bzip2::MAGIC_BITS_BLOCK )
    {}

    explicit
    BlockFinder( const std::string& filePath ) :
        m_bitStringFinder( filePath, bzip2::MAGIC_BITS_BLOCK )
    {}

    ~BlockFinder()
    {
        m_cancelThread = true;
        std::cerr << ( ThreadSafeOutput() << "[Block Finder] Destructor!" ).str();
        std::scoped_lock lock( m_mutex );
        std::cerr << ( ThreadSafeOutput() << "[Block Finder] Cancel thread!" ).str();
        m_changed.notify_all();
    }

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
     * In contrast to StreamedResults::operator[], this call will track the requested block so that the
     * finder loop will look up to that block.
     * Therefore, either a result can be returned or if not it means we are finalized and the requested block
     * is out of range!
     */
    [[nodiscard]] std::optional<size_t>
    get( size_t blockNumber )
    {
        {
            std::scoped_lock lock( m_mutex );
            m_highestRequestedBlockNumber = std::max( m_highestRequestedBlockNumber, blockNumber );
            m_changed.notify_all();
        }

        return m_blockOffsets.get( blockNumber );
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
            m_blockOffsets.push( m_bitStringFinder.find() );

            if constexpr ( m_debugOutput ) {
                /* std::cerr << ( ThreadSafeOutput()
                    << "[Block Finder] Found offset " << m_blockOffsets.get( m_blockOffsets.size() - 1 ).value()
                    << " cancel thread? " << m_cancelThread << " owns lock? " << lock.owns_lock()
                ).str(); */
            }

            lock.unlock(); // Unlock for a little while so that others can acquire the lock!
        }

        m_blockOffsets.finalize();

        if ( m_debugOutput ) {
            std::cerr << ( ThreadSafeOutput() << "[Block Finder] Found" << m_blockOffsets.size() << "blocks" ).str();
            std::cerr << ( ThreadSafeOutput() << "[Block Finder] Shutdown" ).str();
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

    /** @todo ParallelBitStringFinder ? */
    ParallelBitStringFinder<bzip2::MAGIC_BITS_SIZE> m_bitStringFinder;
    std::atomic<bool> m_cancelThread{ false };
    static constexpr bool m_debugOutput = true;

    JoiningThread m_blockFinder{ &BlockFinder::blockFinderMain, this };
};


namespace FetchingStrategy
{
class FetchingStrategy
{
public:
    virtual ~FetchingStrategy() = default;

    virtual void
    fetch( size_t index ) = 0;

    [[nodiscard]] virtual std::vector<size_t>
    prefetch() const = 0;
};


/**
 * @todo Do not prefetch everything at full cores at once as we are not 100% sure!
 * @todo Detect forward and backward seeking
 */
class FetchNext :
    public FetchingStrategy
{
public:
    FetchNext( size_t maxPrefetchCount = std::thread::hardware_concurrency() ) :
        m_maxPrefetchCount( maxPrefetchCount )
    {}

    void
    fetch( size_t index ) override
    {
        previousIndexes.push_back( index );
        while ( previousIndexes.size() > 5 ) {
            previousIndexes.pop_front();
        }
    }

    [[nodiscard]] std::vector<size_t>
    prefetch() const override
    {
        if ( previousIndexes.empty() ) {
            return {};
        }

        /** @todo Stupidly simple prefetcher for now! */
        std::vector<size_t> toPrefetch( m_maxPrefetchCount );
        std::iota( toPrefetch.begin(), toPrefetch.end(), previousIndexes.back() );
        return toPrefetch;
    }

private:
    size_t m_maxPrefetchCount;
    std::deque<size_t> previousIndexes;
};
}


namespace CacheStrategy
{
template<typename Index>
class CacheStrategy
{
public:
    virtual ~CacheStrategy() = default;
    virtual void touch( Index index ) = 0;
    [[nodiscard]] virtual std::optional<Index> evict() = 0;
};


template<typename Index>
class LeastRecentlyUsed :
    public CacheStrategy<Index>
{
public:
    LeastRecentlyUsed() = default;

    void
    touch( Index index ) override
    {
        ++usageNonce;
        auto [match, wasInserted] = m_lastUsage.try_emplace( std::move( index ), usageNonce );
        if ( !wasInserted ) {
            match->second = usageNonce;
        }
    }

    [[nodiscard]] std::optional<Index>
    evict() override
    {
        if ( m_lastUsage.empty() ) {
            return std::nullopt;
        }

        auto lowest = m_lastUsage.begin();
        for ( auto it = std::next( lowest ); it != m_lastUsage.end(); ++it ) {
            if ( it->second < lowest->second ) {
                lowest = it;
            }
        }

        auto indexToEvict = lowest->first;
        m_lastUsage.erase( lowest );
        return indexToEvict;
    }

private:
    /* With this, inserting will be relatively fast but eviction make take longer
     * because we have to go over all elements. */
    std::map<Index, size_t> m_lastUsage;
    size_t usageNonce{ 0 };
};
}


/**
 * Thread-safe cache.
 */
template<
    typename Key,
    typename Value,
    typename CacheStrategy = CacheStrategy::LeastRecentlyUsed<Key>
>
class Cache
{
public:
    using CacheDataType = std::map<Key, Value >;

public:
    Cache( size_t maxCacheSize ) :
        m_maxCacheSize( maxCacheSize )
    {}

    void
    touch( const Key& key )
    {
        m_cacheStrategy.touch( key );
    }

    [[nodiscard]] std::optional<Value>
    get( const Key& key ) const
    {
        std::scoped_lock lock( m_mutex );
        if ( const auto match = m_cache.find( key ); match != m_cache.end() ) {
            m_cacheStrategy.touch( key );
            return match->second;
        }
        return std::nullopt;
    }

    void
    insert( Key   key,
            Value value )
    {
        std::scoped_lock lock( m_mutex );

        while ( m_cache.size() >= m_maxCacheSize ) {
            if ( const auto toEvict = m_cacheStrategy.evict(); toEvict ) {
                m_cache.erase( *toEvict );
            } else {
                m_cache.erase( m_cache.begin() );
            }
        }

        const auto [match, wasInserted] = m_cache.try_emplace( std::move( key ), std::move( value ) );
        if ( !wasInserted ) {
            match->second = std::move( value );
        }

        m_cacheStrategy.touch( key );
    }

private:
    mutable CacheStrategy m_cacheStrategy;
    const size_t m_maxCacheSize;
    CacheDataType m_cache;
    mutable std::mutex m_mutex;
};


/**
 * Generic wrapper for a function call, which caches results of functions calls by the function arguments.
 */
template<typename Result, typename... Arguments>
class CachedFunction
{
    using PackedArguments = std::tuple<Arguments...>;
    using Function = std::function<Result( Arguments... )>;
    using CacheStrategy = CacheStrategy::CacheStrategy<PackedArguments>;

public:
    CachedFunction(
        Function      functionToCache,
        size_t        maxCacheSize,
        CacheStrategy cacheStrategy
    ) :
        m_function( std::move( functionToCache ) ),
        m_cache( maxCacheSize, cacheStrategy )
    {}

    /** Only checks the cache, does not actually evaluate the function! */
    std::shared_ptr<Result>
    try_evaluate( Arguments... arguments ) const
    {
        return m_cache.get( arguments... );
    }

    std::shared_ptr<Result>
    operator()( const Arguments&... arguments ) const
    {
        PackedArguments packedArguments( arguments... );
        if ( auto result = m_cache.get( packedArguments ); result.has_value() ) {
            return std::move( *result );
        }
        auto result = std::make_shared( m_function( arguments... ) );
        m_cache.insert( packedArguments, result );
        return result;
    }

private:
    const Function m_function;
    Cache<PackedArguments, std::shared_ptr<Result> > m_cache;
};


/**
 * Should get block offsets and decoded sizes and will do conversions between decoded and encoded offsets!
 * The idea is that at first any forward seeking should be done using read calls and the read call will
 * push all block information to the BlockMapBuilder. And because ParallelBZ2Reader should not be called from
 * differen threads, there should never be a case that lookups to this function should have to wait for
 * other threads to push data into us!
 * This is used by the docer threads, so it must be thread-safe!
 */
class BlockMap
{
public:
    struct BlockInfo
    {
        /**< each BZ2 block in the stream will be given an increasing index number. */
        size_t blockIndex{ 0 };
        size_t encodedOffsetInBits{ 0 };
        size_t decodedOffsetInBytes{ 0 };
        size_t decodedSizeInBytes{ 0 };
    };

public:
    /** BlockFinder is used to determine which blocks exist and which are still missing! */
    BlockMap( std::shared_ptr<BlockFinder> blockFinder,
              std::map<size_t, size_t>* blockToDataOffsets ) :
        m_blockFinder( std::move( blockFinder ) ),
        m_blockToDataOffsets( blockToDataOffsets )
    {
        if ( !m_blockFinder || ( m_blockToDataOffsets == nullptr ) ) {
            throw std::invalid_argument( "May not give invalid pointers as arguments!" );
        }
    }


    void
    insert( size_t blockOffset,
            size_t size )
    {
        /* If successive value, then simply append */
        if ( m_blockToDataOffsets->empty() || ( blockOffset > m_blockToDataOffsets->rbegin()->first ) ) {
            m_blockToDataOffsets->emplace( blockOffset, m_blockToDataOffsets->rbegin()->second + m_lastBlockSize );
            m_lastBlockSize = size;
            return;
        }

        /* Generally, block inserted offsets should always be increasing!
         * But do ignore duplicates after confirming that there is no data inconsistency. */
        const auto match = m_blockToDataOffsets->find( blockOffset );

        if ( match == m_blockToDataOffsets->end() ) {
            throw std::invalid_argument( "Inserted block offsets should be strictly increasing!" );
        }

        if ( std::next( match ) == m_blockToDataOffsets->end() ) {
            throw std::logic_error( "This case should already be handled with the first if clause!" );
        }

        const auto impliedDecodedSize = std::next( match )->second - match->second;
        if ( impliedDecodedSize != size ) {
            throw std::invalid_argument( "Got duplicate block offset with inconsistent size!" );
        }

        /* Quietly ignore duplicate insertions. */
    }


    /**
     * Returns the block containing the given data offset. May return a block which does not contain the given
     * offset. In that case it will be the last block.
     */
    [[nodiscard]] BlockInfo
    find( size_t dataOffset ) const
    {
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
        /* O(n) */
        result.blockIndex = m_blockToDataOffsets->size() - std::distance( blockOffset, m_blockToDataOffsets->rend() );

        if ( blockOffset != m_blockToDataOffsets->rbegin() ) {
            const auto higherBlock = std::prev( /* reverse! */ blockOffset );
            if ( higherBlock->second < blockOffset->second ) {
                std::logic_error( "Data offsets are not monotonically increasing!" );
            }
            result.decodedSizeInBytes = higherBlock->second - blockOffset->second;
        }

        return result;
    }


    [[nodiscard]] size_t
    size() const
    {
        return m_blockToDataOffsets->size();
    }


    [[nodiscard]] bool
    finalized() const
    {
        return m_blockFinder->finalized() && ( m_blockFinder->size() == m_blockToDataOffsets->size() );
    }

private:
    std::shared_ptr<BlockFinder> const m_blockFinder;
    /** If complete, the last block will be of size 0 and indicate the end of stream! */
    std::map<size_t, size_t>* const m_blockToDataOffsets;

    size_t m_lastBlockSize{ 0 }; /**< Block size of m_blockToDataOffsets.rbegin() */
    size_t m_highestDataOffset{ 0 }; /**< used only for sanity check. */
};



template<typename FetchingStrategy = FetchingStrategy::FetchNext>
class BlockFetcher
{
public:
    struct BlockData
    {
        size_t offsetBitsEncoded  = std::numeric_limits<size_t>::max();
        size_t encodedBitsCount  = 0;

        std::vector<uint8_t> data;

        uint32_t calculatedCRC = 0xFFFFFFFFL;
        uint32_t expectedCRC   = 0; /** if isEndOfStreamBlock == true, then this is the stream CRC. */

        bool isEndOfStreamBlock = false;
    };

public:
    /** @todo might also need BlockMap in order to have a countable index to use for PrefetchStrategy! */
    BlockFetcher( BitReader                 bitReader,
                  std::shared_ptr<BlockMap> blockMap,
                  uint8_t                   blockSize100k = 9,
                  unsigned int              parallelism = 0 ) :
        m_bitReader( std::move( bitReader ) ),
        m_blockMap( std::move( blockMap ) ),
        m_blockSize100k( blockSize100k ),
        m_cache( 3 * m_threadPool.size() ),
        m_threadPool( parallelism == 0 ? std::thread::hardware_concurrency() : parallelism )
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
    get( size_t blockOffset )
    {
        const auto blockInfo = m_blockMap->find( blockOffset );
        if ( ( blockOffset < blockInfo.decodedOffsetInBytes )
             || ( blockOffset >= blockInfo.decodedOffsetInBytes + blockInfo.decodedSizeInBytes ) ) {
            throw std::logic_error( "Block offset should always first be moved into block map before being fetched!" );
        }

        /* First, access cache before data might get evicted! (May return std::nullopt) */
        auto result = m_cache.get( blockOffset );

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
            }
        }

        /* Start requested calculation if necessary. */
        if ( !result && !resultFuture.valid() ) {
            resultFuture = m_threadPool.submitTask( [this, blockOffset](){ return decodeBlock( blockOffset ); } );
        }

        /* Check for ready prefetches, move to cache, and replace with other uncached prefetches. */

        m_fetchingStrategy.fetch( blockInfo.blockIndex );
        auto toPrefetch = m_fetchingStrategy.prefetch();
        toPrefetch.erase( std::remove_if( toPrefetch.begin(), toPrefetch.end(),
                                          [this] ( const auto& offset ) { return m_cache.get( offset ).has_value(); } ),
                          toPrefetch.end() );

        if ( result ) {
            return *result;
        }

        result = std::make_shared<BlockData>( resultFuture.get() );
        m_cache.insert( blockOffset, *result );
        return *result;
    }

#if 0
    void
    workDispatcherMain()
    {
        std::cerr << ( ThreadSafeOutput() << "[Work Dispatcher] Boot" ).str();
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

            /* @todo Kinda hacky. However, this is important in order to rethrow and notice exceptions! */
            for ( auto future = futures.begin(); future != futures.end(); ) {
                if ( future->valid() &&
                     ( future->wait_for( std::chrono::seconds( 0 ) ) == std::future_status::ready ) ) {
                    future->get();
                    future = futures.erase( future );
                } else {
                    ++future;
                }
            }

            /** @todo only decode up to hardware_concurrency blocks, then wait for old ones to be cleared! */
            if ( m_blocks.unprocessedBlockCount() == 0 ) {
                m_blocks.waitUntilChanged( 0.01 ); /* Every 100ms, check whether this thread has been canceled. */
                //std::cerr << ( ThreadSafeOutput() << "[Work Dispatcher] Waiting for new blocks!"
                //               << "Unprocessed tasks in thread pool:" << m_threadPool.unprocessedTasksCount() ).str();
            }
        }

        std::cerr << ( ThreadSafeOutput() << "[Work Dispatcher] Wait on submitted tasks" ).str();
        for ( auto& future : futures ) {
            future.get();
        }
        std::cerr << ( ThreadSafeOutput() << "[Work Dispatcher] Shutdown" ).str();
    }
#endif

    BlockData
    decodeBlock( size_t blockOffset )
    {
        BitReader bitReader( m_bitReader );
        bitReader.seek( blockOffset );
        bzip2::Block block( bitReader );

        BlockData result;
        result.offsetBitsEncoded = blockOffset;
        result.isEndOfStreamBlock = block.eos();
        result.expectedCRC = block.bwdata.headerCRC;

        if ( block.eos() ) {
            std::cerr << ( ThreadSafeOutput() << "EOS block at" << blockOffset ).str();
            result.encodedBitsCount = bitReader.tell() - blockOffset;
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
        result.encodedBitsCount = bitReader.tell() - blockOffset;
        result.calculatedCRC = block.bwdata.dataCRC;

        return result;

#if 0
        /* Check whether the next block is an EOS block, which has a different magic byte string
         * and therefore will not be found by the block finder! Such a block will span 48 + 32 + (0..7) bits.
         * However, the last 0 to 7 bits are only padding and not needed! */
        if ( !bitReader.eof() ) {
            const auto eosBlockOffset = bitReader.tell();
            std::vector<uint8_t> buffer( ( bzip2::MAGIC_BITS_SIZE + 32 ) / CHAR_BIT );
            bitReader.read( reinterpret_cast<char*>( buffer.data() ), buffer.size() );
            uint64_t bits = 0;
            for ( int i = 0; i < bzip2::MAGIC_BITS_SIZE / CHAR_BIT; ++i ) {
                bits = ( bits << CHAR_BIT ) | static_cast<uint64_t>( buffer[i] );
            }
            if ( bits == bzip2::MAGIC_BITS_EOS ) {
                m_blocks.insertBlock( eosBlockOffset, std::move( buffer ) );
            }
        }
#endif
    }

private:
    /* Variables required by decodeBlock and which therefore should be either const or locked. */
    const BitReader m_bitReader;
    const std::shared_ptr<BlockMap> m_blockMap;
    uint8_t m_blockSize100k;

    /** Future holding the number of found magic bytes. Used to determine whether the thread is still running. */
    std::atomic<bool> m_cancelThreads{ false };
    std::condition_variable m_cancelThreadsCondition;

    Cache</** block offset in bits */ size_t, std::shared_ptr<BlockData> > m_cache;
    FetchingStrategy m_fetchingStrategy;

    std::map<size_t, std::future<BlockData> > m_prefetching;
    ThreadPool m_threadPool;
};


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
    using BlockFetcher = ::BlockFetcher<FetchingStrategy::FetchNext>;

public:
#if 0
    /** @todo Add parallelism parameter. */
    template<class... T_Args>
    explicit
    ParallelBZ2Reader( T_Args&&... args ) :
        BZ2Reader( std::forward<T_Args>( args )... ),
        m_blockFinder( &ParallelBZ2Reader::blockFinderMain, this ),
        m_workDispatcher( &ParallelBZ2Reader::workDispatcherMain, this )
    {}
#else
    explicit
    ParallelBZ2Reader( int fileDescriptor ) :
        BZ2Reader( fileDescriptor ),
        m_blockFinder( std::make_shared<BlockFinder>( fileDescriptor ) )
    {}

    ParallelBZ2Reader( const char*  bz2Data,
                       const size_t size ) :
        BZ2Reader( bz2Data, size ),
        m_blockFinder( std::make_shared<BlockFinder>( bz2Data, size ) )
    {}

    ParallelBZ2Reader( const std::string& filePath ) :
        BZ2Reader( filePath ),
        m_blockFinder( std::make_shared<BlockFinder>( filePath ) )
    {}
#endif


public:
    /**
     * Requests data chunks from the BlockDatabase using the current position until we got as much data as requested.
     * The BlockDatabase "analyzes" these requests and prefetches and caches chunks in parallel.
     */
    int
    read( const int    outputFileDescriptor = -1,
          char* const  outputBuffer = nullptr,
          const size_t nBytesToRead = std::numeric_limits<size_t>::max() )
    {
        if ( eof() || ( nBytesToRead == 0 ) ) {
            return 0;
        }

        if ( m_bitReader.tell() == 0 ) {
            readBzip2Header();
            m_blockFetcher = std::make_unique<BlockFetcher>( m_bitReader, m_blockMap, m_blockSize100k, m_parallelism );
        }

        if ( !m_blockFetcher ) {
            throw std::logic_error( "Block fetcher should have been initialized." );
        }

        /** @todo read Bzip2 header for CRC check and stuff. Might be problematic for multiple Bzip2 streams. */
        /** @todo Need to fill BZ2Reader::m_blockToDataOffsets with data from decoded block sizes */

        size_t nBytesDecoded = 0;
        while ( ( nBytesDecoded < nBytesToRead ) && !eof() ) {
            std::shared_ptr<BlockFetcher::BlockData> blockData;

            const auto blockInfo = m_blockMap->find( m_currentPosition );
            if ( blockInfo.decodedOffsetInBytes + blockInfo.decodedSizeInBytes < m_currentPosition ) {
                /* Fetch new block for the first time and add information to block map. */
                const auto encodedOffsetInBits = m_blockFinder->get( m_blockMap->size() );
                if ( !encodedOffsetInBits ) {
                    std::cerr << "EOF reached 2\n";
                    m_atEndOfFile = true;
                    break;
                }

                blockData = m_blockFetcher->get( *encodedOffsetInBits );
                m_blockMap->insert( *encodedOffsetInBits, blockData->data.size() );

                if ( blockData->data.empty() ) {
                    std::cerr << "Encountered empty block. Might happen for EOS blocks.";
                    /* Note that this can be removed because writeResult would be called with 0 anyway. */
                    continue;
                }
            } else {
                blockData = m_blockFetcher->get( blockInfo.encodedOffsetInBits );
                if ( blockData->data.empty() ) {
                    throw std::logic_error( "Block is empty even though it shouldn't be according to block map!" );
                }
            }

            const auto nBytesToDecode = std::min( blockData->data.size(), nBytesToRead - nBytesDecoded );
            nBytesDecoded += writeResult( outputFileDescriptor,
                                          outputBuffer == nullptr ? nullptr : outputBuffer + nBytesDecoded,
                                          reinterpret_cast<const char*>( blockData->data.data() ),
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

        if ( static_cast<long long int>( tell() ) == offset ) {
            return offset;
        }

        /* m_blockMap is only accessed by read and seek, which are not to be called from different threads,
         * so we do not have to lock m_blockMap. */
        const auto blockInfo = m_blockMap->find( offset ); /** @todo use this! */
        /** @todo There shouldn't be much to do except to set m_currentPosition and check against eof! */

        /** @todo use the incomplete data to allow try forward seeking or seek to the furthest position! */
        /* When block offsets are not complete yet, emulate forward seeking with a read. */
        if ( !m_blockToDataOffsetsComplete && ( offset > static_cast<long long int>( tell() ) ) ) {
            read( -1, nullptr, offset - tell() );
            return tell();
        }



        /** @todo emulate forward seeking by reading until we either reach EOF or find the desired offset. */


        /* size() and then seeking requires the block offsets to be available! */
        if ( !m_blockToDataOffsetsComplete ) {
            read();
        }

        offset = std::max<decltype( offset )>( 0, offset );
        m_currentPosition = offset;
#if 0
        //flushOutputBuffer(); // ensure that no old data is left over

        m_atEndOfFile = static_cast<size_t>( offset ) >= size();
        if ( m_atEndOfFile ) {
            return size();
        }

        /* find offset from map (key and values are sorted, so we can bisect!) */
        const auto blockOffset = std::lower_bound(
            m_blockToDataOffsets.rbegin(), m_blockToDataOffsets.rend(), std::make_pair( 0, offset ),
            [] ( std::pair<size_t, size_t> a, std::pair<size_t, size_t> b ) { return a.second > b.second; } );

        if ( ( blockOffset == m_blockToDataOffsets.rend() ) || ( static_cast<size_t>( offset ) < blockOffset->second ) ) {
            throw std::runtime_error( "Could not find block to seek to for given offset" );
        }
        const auto nBytesSeekInBlock = offset - blockOffset->second;

        m_lastHeader = readBlockHeader( blockOffset->first );
        m_lastHeader.readBlockData();
        /* no decodeBzip2 necessary because we only seek inside one block! */
        const auto nBytesDecoded = decodeStream( -1, nullptr, nBytesSeekInBlock );

        if ( nBytesDecoded != nBytesSeekInBlock ) {
            std::stringstream msg;
            msg << "Could not read the required " << nBytesSeekInBlock
            << " to seek in block but only " << nBytesDecoded << "\n";
            throw std::runtime_error( msg.str() );
        }
#endif
        return offset;
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
    const unsigned int m_parallelism{ std::thread::hardware_concurrency() };
    const std::shared_ptr<BlockFinder> m_blockFinder;
    const std::shared_ptr<BlockMap> m_blockMap{ std::make_unique<BlockMap>( m_blockFinder, &m_blockToDataOffsets ) };
    std::unique_ptr<BlockFetcher> m_blockFetcher;
};
