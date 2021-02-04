#pragma once

#include <cassert>
#include <cstdint>
#include <condition_variable>
#include <ostream>
#include <limits>
#include <map>
#include <mutex>
#include <queue>
#include <set>
#include <stdexcept>
#include <utility>
#include <vector>


/**
 * @todo make the BlockData more abstract. E.g., the CRC and the "isEndOfStreamBlock" blocks are very BZ2 specific.
 */
struct BlockData
{
    /** The encoded offset in bits is also used as key in the BlockDatabase map and thereby somewhat redundant. */
    size_t offsetBitsEncoded  = std::numeric_limits<size_t>::max();
    size_t offsetBytesDecoded = std::numeric_limits<size_t>::max();

    /**
     * offsetsBitEncoded + encodedBitsCount should be offsetBitsEncoded of the block right after it.
     * encodedBitsCount can't be derived from encodedData length because it only has byte precision.
     * Furthermore, the data vectors might be empty at all times, so you should not rely on their size!
     */
    size_t encodedBitsCount  = 0;
    size_t decodedBytesCount = 0;

    std::vector<uint8_t> encodedData;
    /** Maybe make it list<vector> to reduce latency between decode and return partial results. */
    std::vector<uint8_t> decodedData; /** @todo use shared pointer here? */

    uint32_t calculatedCRC = 0xFFFFFFFFL;
    uint32_t expectedCRC   = 0; /** if isEndOfStreamBlock == true, then this is the stream CRC. */
    /**
     * @todo imo EOS blocks shouldn't even be added to the BlockDatabase in the first place.
     * They are useless for the BlockDatabase and only make sense for the CRC check, which is quite BZ2 specific.
     */
    bool isEndOfStreamBlock = false;

    /* This future can be waited for when this block's state is PROCESSING. */
    //std::future<void> future;

    /**
     * @todo This state is bascially redundant with the set members in BlockDatabase like m_blocksBeingProcessed.
     * Remove the redundancy! Same for the planned future above. That could be the value to a m_blocksBeingProcessed
     * map.
     */

    enum class State
    {
        UNDECODED,
        PROCESSING,
        PROCESSED,
    };

    State state = State::UNDECODED;
};


std::ostream&
operator<<( std::ostream&    out,
            const BlockData& block )
{
    out << "BlockData {\n";
    out << "  offsetBitsEncoded  : " << block.offsetBitsEncoded  << "\n";
    out << "  offsetBytesDecoded : " << block.offsetBytesDecoded << "\n";
    out << "  encodedBitsCount   : " << block.encodedBitsCount   << "\n";
    out << "  decodedBytesCount  : " << block.decodedBytesCount  << "\n";
    out << "  encodedData.size() : " << block.encodedData.size() << "\n";
    out << "  decodedData.size() : " << block.decodedData.size() << "\n";
    out << "  calculatedCRC      : " << block.calculatedCRC      << "\n";
    out << "  expectedCRC        : " << block.expectedCRC        << "\n";
    out << "  isEndOfStreamBlock : " << block.isEndOfStreamBlock << "\n";
    out << "}\n";

    return out;
}


/**
 * The block database is a structure which at its heart was just the map of offsets in the encoded to the
 * offsets in the decoded stream. However, for parallelisation, it grew to also contain other data for
 * management and communication between the threads.
 */
class BlockDatabase :
    public std::map<size_t, BlockData>
{
public:
    using BaseType = std::map<size_t, BlockData>;

public:
    BlockDatabase() = default;

    [[nodiscard]] bool
    completed() const
    {
        return m_state == State::COMPLETE;
    }

    [[nodiscard]] std::map<size_t, size_t>
    blockOffsets() const
    {
        std::lock_guard guard( m_mutex );

        if ( !completed() ) {
            throw std::invalid_argument( "The block database is not complete yet. Can't return offsets!" );
        }

        std::map<size_t, size_t> encodedToDecoded;
        for ( const auto& [offsetBitsEncoded, blockData] : *this ) {
            encodedToDecoded.emplace( offsetBitsEncoded, blockData.offsetBytesDecoded );
        }
        return encodedToDecoded;
    }

    void
    setBlockOffsets( const std::map<size_t, size_t>& encodedToDecoded )
    {
        if ( !empty() ) {
            throw std::invalid_argument( "Block offsets may only be set once!" );
        }

        if ( encodedToDecoded.empty() ) {
            return;
        }

        if ( encodedToDecoded.begin()->second != 0 ) {
            throw std::invalid_argument( "The first decoded block offset must be 0!" );
        }

        std::lock_guard guard( m_mutex );
        size_t lastOffsetBitsEncoded = 0;
        size_t lastOffsetBytesDecoded = 0;
        bool firstBlock = true;
        for ( const auto& [offsetBitsEncoded, offsetBytesDecoded] : encodedToDecoded ) {
            if ( !firstBlock ) {
                BlockData blockData;

                blockData.offsetBitsEncoded = lastOffsetBitsEncoded;
                blockData.offsetBytesDecoded = lastOffsetBytesDecoded;

                blockData.encodedBitsCount = offsetBitsEncoded - lastOffsetBitsEncoded;
                blockData.decodedBytesCount = offsetBytesDecoded - lastOffsetBytesDecoded;

                emplace( lastOffsetBitsEncoded, std::move( blockData ) );
            }

            lastOffsetBitsEncoded = offsetBitsEncoded;
            lastOffsetBytesDecoded = offsetBytesDecoded;
            firstBlock = false;
        }

        m_changed.notify_all();
    }

    /**
     * This is e.g. used by the block finder thread, which only looks for BZ2 blocks based on the magic bit string.
     */
    void
    insertBlock( size_t                 offsetBitsEncoded,
                 std::vector<uint8_t>&& encodedData = {} )
    {
        std::lock_guard guard( m_mutex );

        if ( completed() ) {
            throw std::invalid_argument( "Block database already marked as complete. You may not insert blocks!" );
        }

        const auto [it, wasInserted] = emplace( offsetBitsEncoded, BlockData{} );
        if ( !wasInserted ) {
            /* Block already existed. */
            return;
        }

        it->second.encodedData = std::move( encodedData );
        it->second.offsetBitsEncoded = offsetBitsEncoded;
        if ( it == begin() ) {
            it->second.offsetBytesDecoded = 0;
        }

        m_unprocessedBlocks.push( offsetBitsEncoded );
        m_changed.notify_all();
    }

    [[nodiscard]] size_t
    unprocessedBlockCount() const
    {
        return m_unprocessedBlocks.size();
    }

    [[nodiscard]] size_t
    blocksBeingProcessedCount() const
    {
        return m_blocksBeingProcessed.size();
    }

    [[nodiscard]] size_t
    blocksWithDataCount() const
    {
        return m_blocksWithData.size();
    }

    size_t
    takeBlockForProcessing()
    {
        std::lock_guard guard( m_mutex );

        if ( completed() ) {
            throw std::invalid_argument( "Block database already complete. There are no unprocessed blocks!" );
        }

        if ( m_unprocessedBlocks.empty() ) {
            return std::numeric_limits<size_t>::max();
        }

        size_t offset = m_unprocessedBlocks.front();
        m_unprocessedBlocks.pop();
        m_blocksBeingProcessed.insert( offset );

        assert( at( offset ).state == BlockData::State::UNDECODED );
        at( offset ).state = BlockData::State::PROCESSING;

        m_changed.notify_all();

        return offset;
    }

    void
    setBlockData( size_t                 offsetBitsEncoded,
                  size_t                 encodedBitsCount,
                  std::vector<uint8_t>&& decodedData,
                  uint32_t               calculatedCRC,
                  uint32_t               expectedCRC,
                  bool                   isEndOfStreamBlock )
    {
        std::lock_guard guard( m_mutex );

        if ( completed() ) {
            throw std::invalid_argument( "Block database already marked as complete. You may not modify it!" );
        }

        auto& block = this->operator[]( offsetBitsEncoded );

        block.offsetBitsEncoded  = offsetBitsEncoded;
        block.encodedBitsCount   = encodedBitsCount;
        block.decodedBytesCount  = decodedData.size();
        block.encodedData        = std::vector<uint8_t>(); /* Note that clear will not free allocated memory! */
        block.decodedData        = std::move( decodedData );
        block.calculatedCRC      = calculatedCRC;
        block.expectedCRC        = expectedCRC;
        block.isEndOfStreamBlock = isEndOfStreamBlock;
        block.state              = BlockData::State::PROCESSED;

        updateDecodedOffsets( offsetBitsEncoded );

        m_blocksBeingProcessed.erase( offsetBitsEncoded );
        if ( !block.decodedData.empty() ) {
            m_blocksWithData.insert( offsetBitsEncoded );
        }

        /** @todo check stream CRC */

        m_changed.notify_all();
    }

    /**
     * An overload which should be used when redecoding already seen blocks.
     */
    void
    setBlockData( size_t                 offsetBitsEncoded,
                  std::vector<uint8_t>&& decodedData,
                  uint32_t               calculatedCRC )
    {
        std::lock_guard guard( m_mutex );

        auto& block = at( offsetBitsEncoded );

        if ( block.isEndOfStreamBlock ) {
            if ( decodedData.empty() ) {
                return;
            }
            throw std::invalid_argument( "End of stream blocks do not contain data!" );
        }

        /* Compare with previously calculated and expected CRC in order to work with broken bzip2 archives */
        if ( ( calculatedCRC != block.expectedCRC ) && ( calculatedCRC != block.calculatedCRC ) ) {
            throw std::invalid_argument( "Data CRC does neither match the bz2 CRC nor the previously calculated one!" );
        }

        block.decodedData = std::move( decodedData );
        if ( !block.decodedData.empty() ) {
            m_blocksWithData.insert( offsetBitsEncoded );
        }

        m_changed.notify_all();
    }

    /**
     * If the block turns out to not be a bzip2 block, which can happen when relying on the magic bytes to find them.
     */
    void
    erase( size_t offsetBitsEncoded )
    {
        std::lock_guard guard( m_mutex );

        if ( completed() ) {
            throw std::invalid_argument( "Block database already marked as complete. You may not remove blocks!" );
        }

        /* In order to get the offsetBitsEncoded, the block should already have been removed
         * from m_unprocessedBlocks by the call to takeBlockForProcessing. */
        m_blocksBeingProcessed.erase( offsetBitsEncoded );
        m_blocksWithData.erase( offsetBitsEncoded ); // This is only for safety. Theoretically it should not happen.
        BaseType::erase( offsetBitsEncoded );

        updateDecodedOffsets( offsetBitsEncoded );
        m_changed.notify_all();
    }

    void
    finalize()
    {
        std::unique_lock<std::mutex> lock( m_mutex );
        m_state = State::FINALIZING;

        if ( !m_unprocessedBlocks.empty() || !m_blocksBeingProcessed.empty() ) {
            m_changed.wait( lock, [this] () { return m_unprocessedBlocks.empty() && m_blocksBeingProcessed.empty(); } );
        }

        for ( auto& [offsetBitsEncoded, block] : *this ) {
            if ( block.state != BlockData::State::PROCESSED ) {
                throw std::logic_error( "Found unprocessed block even though we waited for all to be done!" );
            }

            if ( ( block.offsetBitsEncoded  == std::numeric_limits<size_t>::max() ) ||
                 ( block.offsetBytesDecoded == std::numeric_limits<size_t>::max() ) ||
                 ( block.encodedBitsCount   == 0 ) )
            {
                std::stringstream msg;
                msg << "Found block with invalid data during finalizing!\n";
                msg << block;
                throw std::logic_error( msg.str() );
            }

            if ( block.isEndOfStreamBlock != ( block.decodedBytesCount == 0 ) ) {
                std::stringstream msg;
                msg << "Found non EOS block without any payload!\n";
                msg << block;
                throw std::logic_error( msg.str() );
            }

            block.encodedData = std::vector<uint8_t>(); // clear does not free the allocated memory!
        }

        m_state = State::COMPLETE;
        m_changed.notify_all();
    }

    void
    waitUntilChanged( double timeoutSeconds = 0 )
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


    /**
     * Clears internal decoded and encoded buffers if non-empty for all blocks before (not including)
     * the given offset in the decoded data. Should be called regularly to avoid unlimited memory growth.
     */
    void
    clearBlockDataBefore( size_t offsetBytesDecoded )
    {
        std::lock_guard guard( m_mutex );
        for ( auto offsetBitsEncoded = m_blocksWithData.begin(); offsetBitsEncoded != m_blocksWithData.end(); ) {
            auto& block = at( *offsetBitsEncoded );
            if ( ( block.offsetBytesDecoded != std::numeric_limits<size_t>::max() ) &&
                 ( block.offsetBytesDecoded + block.decodedBytesCount < offsetBytesDecoded ) ) {
                block.encodedData = std::vector<uint8_t>(); // clear does not free the allocated memory!
                block.decodedData = std::vector<uint8_t>(); // clear does not free the allocated memory!
                offsetBitsEncoded = m_blocksWithData.erase( offsetBitsEncoded );
            } else {
                ++offsetBitsEncoded;
            }
        }
        m_changed.notify_all();
    }

    void
    clearBlockData( size_t offsetBitsEncoded )
    {
        std::lock_guard guard( m_mutex );
        auto& block = at( offsetBitsEncoded );
        block.encodedData = std::vector<uint8_t>(); // clear does not free the allocated memory!
        block.decodedData = std::vector<uint8_t>(); // clear does not free the allocated memory!
        m_changed.notify_all();
    }

    /**
     * @return pointer to buffer at the given offset and the size of the returned buffer.
     */
    [[nodiscard]] std::pair<const uint8_t*, size_t>
    data( size_t offsetBytesDecoded ) const
    {
        std::unique_lock lock( m_mutex );

        const BlockData* blockContainingOffset = nullptr;

        while ( true ) {
            if ( completed() ) {
                BlockData blockDataToLookFor;
                blockDataToLookFor.offsetBytesDecoded = offsetBytesDecoded;
                /** @todo Use bisection lower_bound if completed for performance. */
                /* const auto block = std::lower_bound(
                                       rbegin(), rend(),
                                       std::make_pair( 0, blockDataToLookFor ),
                                       [] ( const auto& a, const auto& b ) {
                                           return a.second.offsetBytesDecoded < b.second.offsetBytesDecoded;
                                       } ); */
                const auto block = std::find_if(
                    rbegin(), rend(),
                    [offsetBytesDecoded] ( const auto& x ) {
                        return ( x.second.offsetBytesDecoded != std::numeric_limits<size_t>::max() )
                               && ( offsetBytesDecoded >= x.second.offsetBytesDecoded )
                               && ( x.second.decodedBytesCount > 0 )
                               && ( x.second.state == BlockData::State::PROCESSED );
                    } );

                if ( block == rend() ) {
                    return { nullptr, 0 };
                }

                blockContainingOffset = &block->second;
                break;
            }

            /* Use linear search to find fitting offset and if not found wait for new data.
             * Bisection won't work because some of the blocks might still be uninitialized and
             * therefore the offsetBytesDecoded won't be monotonically increasing. */
            const auto block = std::find_if(
                rbegin(), rend(),
                [offsetBytesDecoded] ( const auto& x ) {
                    return ( x.second.offsetBytesDecoded != std::numeric_limits<size_t>::max() )
                           && ( offsetBytesDecoded >= x.second.offsetBytesDecoded )
                           && ( x.second.decodedBytesCount > 0 )
                           && ( x.second.state == BlockData::State::PROCESSED );
                } );

            if ( block != rend() ) {
                blockContainingOffset = &block->second;
                break;
            }

            m_changed.wait( lock );
        }

        /** @todo if decodedData is not set, start thread to decode it? */
        const auto offsetInBlock = offsetBytesDecoded - blockContainingOffset->offsetBytesDecoded;
        if ( offsetInBlock >= blockContainingOffset->decodedData.size() ) {
            /* This may happen when the reader threads wants new data which has not been decoded yet! */
            return { nullptr, 0 };
        }
        return { blockContainingOffset->decodedData.data() + offsetInBlock,
                 blockContainingOffset->decodedData.size() - offsetInBlock };
    }


private:
    /**
     * Calculate the offset in the decoded data if possible as the cumulative sum over all blocks before.
     * Also checks whether now that this block has been updated, the blocks after it might be updatable.
     *
     * @note This is a private method and should only be called with the mutex locked!
     */
    void
    updateDecodedOffsets( size_t firstEncodedOffsetToUpdate )
    {
        auto block = lower_bound( firstEncodedOffsetToUpdate );
        auto prevBlock = block;
        for ( prevBlock == begin() ? ++block : --prevBlock;
              ( block != end() )
              && ( prevBlock->second.state              == BlockData::State::PROCESSED )
              && ( prevBlock->second.offsetBytesDecoded != std::numeric_limits<size_t>::max() )
              && ( prevBlock->second.decodedBytesCount  != std::numeric_limits<size_t>::max() )
              && ( prevBlock->second.offsetBitsEncoded + prevBlock->second.encodedBitsCount
                   == block->second.offsetBitsEncoded );
              ++block, ++prevBlock )
        {
            block->second.offsetBytesDecoded = prevBlock->second.offsetBytesDecoded +
                                               prevBlock->second.decodedBytesCount;
        }
    }

private:
    mutable std::mutex m_mutex;
    mutable std::condition_variable m_changed;

    enum class State
    {
        INCOMPLETE,

        /** When finalizing, no further calls to insertBlock should be done! */
        FINALIZING,

        /**
         * Indicates that no more blocks will be added and that at least offsetBitsEncoded, offsetBytesDecoded,
         * encodedBitsCount, and encodedBytesCount are set for all blocks.
         */
        COMPLETE,
    };

    State m_state = State::INCOMPLETE;

    /**
     * These blocks were never processed once, meaning they still have unitialized decoded offsets or might
     * even be invalid. Also they might have buffers of encoded data which can be large!
     */
    std::queue<size_t> m_unprocessedBlocks;

    /** These blocks were taken by someone and are currently "locked". @todo Actually use a block-level lock. */
    std::set<size_t> m_blocksBeingProcessed;

    /** These blocks have decoded data and should be cleared when not needed any longer. */
    std::set<size_t> m_blocksWithData;
};
