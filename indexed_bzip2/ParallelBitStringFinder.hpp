#pragma once

#include <climits>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <optional>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <unistd.h>         // dup, fileno

#include "BitReader.hpp"
#include "BitStringFinder.hpp"
#include "ThreadPool.hpp"


namespace
{
template<typename I1,
         typename I2,
         typename Enable = typename std::enable_if<
            std::is_integral<I1>::value &&
            std::is_integral<I2>::value
         >::type>
I1
ceilDiv( I1 dividend,
         I2 divisor )
{
    return ( dividend + divisor - 1 ) / divisor;
}
}


/**
 * No matter the input, the data is read from an input buffer.
 * If a file is given, then that input buffer will be refilled when the input buffer empties.
 * It is less a file object and acts more like an iterator.
 * It offers a @ref find method returning the next match or std::numeric_limits<size_t>::max() if the end was reached.
 */
template<size_t bitStringCount>
class ParallelBitStringFinder
{
public:
    using BitStringsContainer = std::array<uint64_t, bitStringCount>;

public:
    ParallelBitStringFinder( std::string         filePath,
                             BitStringsContainer bitStringsToFind,
                             uint8_t             bitStringSize,
                             size_t              parallelisation = std::thread::hardware_concurrency(),
                             size_t              requestedBytes = 0,
                             size_t              fileBufferSizeBytes = 1*1024*1024 ) :
        m_file             ( std::fopen( filePath.c_str(), "rb" ) ),
        m_fileChunksInBytes( chunkSize( fileBufferSizeBytes, bitStringSize, requestedBytes, parallelisation ) ),
        m_bitStringsToFind ( maskBitStrings( bitStringsToFind, bitStringSize ) ),
        m_bitStringSize    ( bitStringSize )
    {
        if ( seekable() ) {
            fseek( m_file, 0, SEEK_SET );
        }
    }

    ParallelBitStringFinder( int                 fileDescriptor,
                             BitStringsContainer bitStringsToFind,
                             uint8_t             bitStringSize,
                             size_t              parallelisation = std::thread::hardware_concurrency(),
                             size_t              requestedBytes = 0,
                             size_t              fileBufferSizeBytes = 1*1024*1024 ) :
        m_file             ( fdopen( dup( fileDescriptor ), "rb" ) ),
        m_fileChunksInBytes( chunkSize( fileBufferSizeBytes, bitStringSize, requestedBytes, parallelisation ) ),
        m_bitStringsToFind ( maskBitStrings( bitStringsToFind, bitStringSize ) ),
        m_bitStringSize    ( bitStringSize )
    {
        if ( seekable() ) {
            fseek( m_file, 0, SEEK_SET );
        }
    }

    ParallelBitStringFinder( const char*         buffer,
                             size_t              size,
                             BitStringsContainer bitStringsToFind,
                             uint8_t             bitStringSize,
                             size_t              requestedBytes = 0 ) :
        m_buffer          ( buffer, buffer + size ),
        m_bitStringsToFind( maskBitStrings( bitStringsToFind, bitStringSize ) ),
        m_bitStringSize   ( bitStringSize )
    {}

    /**
     * Idea:
     *   1. Load one chunk if first iteration
     *   2. Use the serial BitStringFinder in parallel on equal-sized sized sub chunks.
     *   3. Filter out results we already could have found in the chunk before if more than bitStringSize-1
     *      bits were loaded from it.
     *   4. Translate the returned bit offsets of the BitStringFinders to global offsets.
     *   5. Copy requested bytes after match into result buffer.
     *   6. Load the next chunk plus at least the last bitStringSize-1 bits from the chunk before.
     *   7. Use that new chunk to append more of the requested bytes after matches to the result buffer.
     *      More than one chunk should not be necessary for this! This is ensured in the chunkSize method.
     *
     * @return the next match and the requested bytes or nullopt if at end of file.
     */
    std::optional<std::pair<size_t, BitReader> >
    find()
    {
        while ( !eof() )
        {
            if ( /* chunk available */ ) {
                /* separate it and push in thread pool */
                /* collect results, translate them, and put into m_matchOffsets */
            }

            /* load next chunk */
        }

        return std::nullopt;
    }

    [[nodiscard]] bool
    seekable() const
    {
        if ( m_file == nullptr ) {
            return true;
        }

        struct stat fileStats;
        fstat( ::fileno( m_file ), &fileStats );
        return !S_ISFIFO( fileStats.st_mode );
    }

    bool
    eof() const
    {
        if ( m_file != nullptr ) {
            return m_buffer.empty() && std::feof( m_file );
        }
        return m_buffer.empty();
    }

private:
    [[nodiscard]] static size_t
    chunkSize( size_t  fileBufferSizeBytes,
               uint8_t bitStringSize,
               size_t  requestedBytes,
               size_t  parallelisation )
    {
        /* This implementation has the limitation that it might at worst try to read as many as bitStringSize
         * bits from the buffered chunk. It makes no sense to remove this limitation. It might slow things down. */
        auto result = std::max( fileBufferSizeBytes,
                                static_cast<size_t>( ceilDiv( bitStringSize, 8 ) ) * parallelisation )
        /* With the current implementation it is impossible to have a chunk size smaller than the requested bytes
         * and have it work for non-seekable inputs. In the worst case, the bit string is at the end, so we have to
         * read almost everything of the next chunk. */
        return std::max( result, requestedBytes );
    }

    /**
     * @verbatim
     * 63                48                  32                  16        8         0
     * |                 |                   |                   |         |         |
     * 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 1111 1111 1111
     *                                                                  <------------>
     *                                                                    length = 12
     * @endverbatim
     *
     * @param length the number of lowest bits which should be 1 (rest are 0)
     */
    static constexpr uint64_t
    mask( uint8_t length )
    {
        return ~static_cast<uint64_t>( 0 ) >> ( sizeof( uint64_t ) * CHAR_BIT - length );
    }

    /** Sets all bits not inside the the specified bit string size to zero. */
    static constexpr BitStringsContainer
    maskBitStrings( const BitStringsContainer& bitStrings,
                    uint8_t                    bitStringSize )
    {
        if ( bitStrings.empty() ) {
            throw std::invalid_argument( "Need at least one bit string!" );
        }

        BitStringsContainer masked;
        for ( size_t i = 0; i < bitStrings.size(); ++i ) {
            masked[i] = bitStrings[i] & mask( bitStringSize );
        }

        return masked;
    }

private:
    std::FILE* m_file = nullptr;
    /** This is not the current size of @ref m_buffer but the number of bytes to read from @ref m_file if it is empty */
    const size_t m_fileChunksInBytes = 1*1024*1024;
    Buffer m_buffer;

    const BitStringsContainer m_bitStringsToFind;
    const uint8_t m_bitStringSize;

    /** Return at least this amount of bytes after and including the found bit strings. */
    const size_t requestedBytes = 0;

    ThreadPool m_threadPool;
};
