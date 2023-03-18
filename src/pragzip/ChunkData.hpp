#pragma once

#include <algorithm>
#include <cmath>
#include <limits>
#include <memory>
#include <stdexcept>
#include <vector>

#include <DecodedData.hpp>


namespace pragzip
{
/**
 * This class adds higher-level capabilities onto @ref deflate::DecodedData, which was only intended for
 * returning decompression results and aggregating them during decompression of a single deflate block.
 * This class instead is intended to aggregate results from multiple deflate blocks, possibly even multiple
 * gzip streams. It is used to hold the chunk data for parallel decompression.
 * It also adds some further metadata like deflate block and stream boundaries and helpers for creating
 * evenly distributed checkpoints for a gzip seek index.
 */
struct ChunkData :
    public deflate::DecodedData
{
    struct BlockBoundary
    {
        size_t encodedOffset;
        size_t decodedOffset;

        [[nodiscard]] bool
        operator==( const BlockBoundary& other ) const
        {
            return ( encodedOffset == other.encodedOffset ) && ( decodedOffset == other.decodedOffset );
        }
    };

    struct Footer
    {
        BlockBoundary blockBoundary;
        gzip::Footer gzipFooter;
    };

    struct Subblock
    {
        size_t encodedOffset{ 0 };
        size_t encodedSize{ 0 };
        size_t decodedSize{ 0 };

        [[nodiscard]] bool
        operator==( const Subblock& other ) const
        {
            return ( encodedOffset == other.encodedOffset )
                   && ( encodedSize == other.encodedSize )
                   && ( decodedSize == other.decodedSize );
        }
    };

public:
    [[nodiscard]] bool
    matchesEncodedOffset( size_t offset )
    {
        if ( maxEncodedOffsetInBits == std::numeric_limits<size_t>::max() ) {
            return offset == encodedOffsetInBits;
        }
        return ( encodedOffsetInBits <= offset ) && ( offset <= maxEncodedOffsetInBits );
    }

    void
    setEncodedOffset( size_t offset );

    [[nodiscard]] std::vector<Subblock>
    split( [[maybe_unused]] const size_t spacing ) const;

    void
    finalize( size_t blockEndOffsetInBits )
    {
        cleanUnmarkedData();
        encodedSizeInBits = blockEndOffsetInBits - encodedOffsetInBits;
        decodedSizeInBytes = deflate::DecodedData::size();
    }

    [[nodiscard]] constexpr size_t
    size() const noexcept = delete;

    [[nodiscard]] constexpr size_t
    dataSize() const noexcept = delete;

public:
    /* This should only be evaluated when it is unequal std::numeric_limits<size_t>::max() and unequal
     * Base::encodedOffsetInBits. Then, [Base::encodedOffsetInBits, maxEncodedOffsetInBits] specifies a valid range
     * for the block offset. Such a range might happen for finding uncompressed deflate blocks because of the
     * byte-padding. */
    size_t maxEncodedOffsetInBits{ std::numeric_limits<size_t>::max() };
    /* Initialized with size() after thread has finished writing into ChunkData. Redundant but avoids a lock
     * because the marker replacement will momentarily lead to different results returned by size! */
    size_t decodedSizeInBytes{ std::numeric_limits<size_t>::max() };

    /* Decoded offsets are relative to the decoded offset of this ChunkData because that might not be known
     * during first-pass decompression. */
    std::vector<BlockBoundary> blockBoundaries;
    std::vector<Footer> footers;

    /* Benchmark results */
    double blockFinderDuration{ 0 };
    double decodeDuration{ 0 };
    double appendDuration{ 0 };
};


void
ChunkData::setEncodedOffset( size_t offset )
{
    if ( !matchesEncodedOffset( offset ) ) {
        throw std::invalid_argument( "The real offset to correct to should lie inside the offset range!" );
    }

    if ( maxEncodedOffsetInBits == std::numeric_limits<size_t>::max() ) {
        maxEncodedOffsetInBits = encodedOffsetInBits;
    }

    /* Correct the encoded size "assuming" (it must be ensured!) that it was calculated from
     * maxEncodedOffsetInBits. */
    encodedSizeInBits += maxEncodedOffsetInBits - offset;

    encodedOffsetInBits = offset;
    maxEncodedOffsetInBits = offset;
}


[[nodiscard]] std::vector<ChunkData::Subblock>
ChunkData::split( [[maybe_unused]] const size_t spacing ) const
{
    if ( encodedOffsetInBits != maxEncodedOffsetInBits ) {
        throw std::invalid_argument( "ChunkData::split may only be called after setEncodedOffset!" );
    }

    if ( spacing == 0 ) {
        throw std::invalid_argument( "Spacing must be a positive number of bytes." );
    }

    /* blockBoundaries does not contain the first block begin but all thereafter including the boundary after
     * the last block, i.e., the begin of the next deflate block not belonging to this ChunkData. */
    const auto decompressedSize = decodedSizeInBytes;
    const auto nBlocks = static_cast<size_t>( std::round( static_cast<double>( decompressedSize )
                                                          / static_cast<double>( spacing ) ) );
    if ( ( nBlocks <= 1 ) || blockBoundaries.empty() ) {
        Subblock subblock;
        subblock.encodedOffset = encodedOffsetInBits;
        subblock.encodedSize = encodedSizeInBits;
        subblock.decodedSize = decompressedSize;
        if ( ( encodedSizeInBits == 0 ) && ( decompressedSize == 0 ) ) {
            return {};
        }
        return { subblock };
    }

    /* The idea for partitioning is: Divide the size evenly and into subblocks and then choose the block boundary
     * that is closest to that value. */
    const auto perfectSpacing = static_cast<double>( decompressedSize ) / static_cast<double>( nBlocks );

    std::vector<BlockBoundary> selectedBlockBoundaries;
    selectedBlockBoundaries.reserve( nBlocks + 1 );
    selectedBlockBoundaries.push_back( BlockBoundary{ encodedOffsetInBits, 0 } );
    /* The first and last boundaries are static, so we only need to find nBlocks - 1 further boundaries. */
    for ( size_t iSubblock = 1; iSubblock < nBlocks; ++iSubblock ) {
        const auto perfectDecompressedOffset = static_cast<size_t>( iSubblock * perfectSpacing );
        const auto isCloser =
            [perfectDecompressedOffset] ( const auto& b1, const auto& b2 )
            {
                return absDiff( b1.decodedOffset, perfectDecompressedOffset )
                       < absDiff( b2.decodedOffset, perfectDecompressedOffset );
            };
        const auto closest = std::min_element( blockBoundaries.begin(), blockBoundaries.end(), isCloser );
        selectedBlockBoundaries.emplace_back( *closest );
    }
    selectedBlockBoundaries.push_back( BlockBoundary{ encodedOffsetInBits + encodedSizeInBits, decompressedSize } );

    /* Clean up duplicate boundaries, which might happen for very large deflate blocks.
     * Note that selectedBlockBoundaries should already be sorted because we push always the closest
     * of an already sorted "input vector". */
    selectedBlockBoundaries.erase( std::unique( selectedBlockBoundaries.begin(), selectedBlockBoundaries.end() ),
                                   selectedBlockBoundaries.end() );

    /* Convert subsequent boundaries into blocks. */
    std::vector<Subblock> subblocks( selectedBlockBoundaries.size() - 1 );
    for ( size_t i = 0; i + 1 < selectedBlockBoundaries.size(); ++i ) {
        assert( selectedBlockBoundaries[i + 1].encodedOffset > selectedBlockBoundaries[i].encodedOffset );
        assert( selectedBlockBoundaries[i + 1].decodedOffset > selectedBlockBoundaries[i].decodedOffset );

        subblocks[i].encodedOffset = selectedBlockBoundaries[i].encodedOffset;
        subblocks[i].encodedSize = selectedBlockBoundaries[i + 1].encodedOffset
                                   - selectedBlockBoundaries[i].encodedOffset;
        subblocks[i].decodedSize = selectedBlockBoundaries[i + 1].decodedOffset
                                   - selectedBlockBoundaries[i].decodedOffset;
    }

    return subblocks;
}


/**
 * Tries to use writeAllSpliceUnsafe and, if successful, also extends lifetime by adding the block data
 * shared_ptr into a list.
 *
 * @note Limitations:
 *  - To avoid querying the pipe buffer size, it is only done once. This might introduce subtle errors when it is
 *    dynamically changed after this point.
 *  - The lifetime can only be extended on block granularity even though chunks would be more suited.
 *    This results in larger peak memory than strictly necessary.
 *  - In the worst case we would read only 1B out of each block, which would extend the lifetime
 *    of thousands of large blocks resulting in an out of memory issue.
 *    - This would only be triggerable by using the API. The current CLI and not even the Python
 *      interface would trigger this because either they don't splice to a pipe or only read
 *      sequentially.
 * @note It *does* account for pages to be spliced into yet another pipe buffer. This is exactly what the
 *       SPLICE_F_GIFT flag is for. Without that being set, pages will not be spliced but copied into further
 *       pipe buffers. So, without this flag, there is no danger of extending the lifetime of those pages
 *       arbitarily.
 */
[[nodiscard]] bool
writeAllSplice( const int                         outputFileDescriptor,
                const void* const                 dataToWrite,
                size_t const                      dataToWriteSize,
                const std::shared_ptr<ChunkData>& chunkData )
{
#if defined( HAVE_VMSPLICE )
    return SpliceVault::getInstance( outputFileDescriptor ).first->splice( dataToWrite, dataToWriteSize, chunkData );
#else
    return false;
#endif
}


#if defined( HAVE_VMSPLICE )
[[nodiscard]] bool
writeAllSplice( [[maybe_unused]] const int                         outputFileDescriptor,
                [[maybe_unused]] const std::shared_ptr<ChunkData>& chunkData,
                [[maybe_unused]] const std::vector<::iovec>&       buffersToWrite )
{
    return SpliceVault::getInstance( outputFileDescriptor ).first->splice( buffersToWrite, chunkData );
}
#endif  // HAVE_VMSPLICE


void
writeAll( const std::shared_ptr<ChunkData>& chunkData,
          const int                         outputFileDescriptor,
          const size_t                      offsetInBlock,
          const size_t                      dataToWriteSize )
{
    if ( ( outputFileDescriptor < 0 ) || ( dataToWriteSize == 0 ) ) {
        return;
    }

#ifdef HAVE_IOVEC
    const auto buffersToWrite = toIoVec( *chunkData, offsetInBlock, dataToWriteSize );
    if ( !writeAllSplice( outputFileDescriptor, chunkData, buffersToWrite ) ) {
        writeAllToFdVector( outputFileDescriptor, buffersToWrite );
    }
#else
    using pragzip::deflate::DecodedData;

    bool splicable = true;
    for ( auto it = DecodedData::Iterator( *chunkData, offsetInBlock, dataToWriteSize );
          static_cast<bool>( it ); ++it )
    {
        const auto& [buffer, size] = *it;
        if ( splicable ) {
            splicable = writeAllSplice( outputFileDescriptor, buffer, size, chunkData );
        }
        if ( !splicable ) {
            writeAllToFd( outputFileDescriptor, buffer, size);
        }
    }
#endif
}
}  // namespace pragzip
