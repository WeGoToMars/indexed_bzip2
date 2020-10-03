#include "bzip2.hpp"

#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <iostream>
#include <limits>
#include <string>
#include <type_traits>
#include <utility>

//#define BENCHMARK

#if 0
/**
 * @param bitString the lowest bitStringSize bits will be looked for in the buffer
 * @return size_t max if not found else position in buffer
 */
size_t
findBitString( const char* buffer,
               size_t      bufferSize,
               uint64_t    bitString,
               uint8_t     bitStringSize )
{
    if ( bufferSize * 8 < bitStringSize ) {
        return std::numeric_limits<size_t>::max();
    }
    assert( bufferSize % sizeof( uint64_t ) == 0 );

    /* create lookup table */
    const auto nWildcardBits = sizeof( uint64_t ) * 8 - bitStringSize;
    std::vector<uint64_t> shiftedBitStrings( nWildcardBits );
    std::vector<uint64_t> shiftedBitMasks( nWildcardBits );

    uint64_t shiftedBitString = bitString;
    uint64_t shiftedBitMask = std::numeric_limits<uint64_t>::max() >> nWildcardBits;
    for ( size_t i = 0; i < nWildcardBits; ++i ) {
        shiftedBitStrings[i] = shiftedBitString;
        shiftedBitMasks  [i] = shiftedBitMask;

        shiftedBitString <<= 1;
        shiftedBitMask   <<= 1;
    }

    /**
     *  0  1  2  3  4  5  6  7  8  9
     *  42 5a 68 31 31 41 59 26 53 59
     */
    const auto minBytesForSearchString = ( (size_t)bitStringSize + 8U - 1U ) / 8U;
    uint64_t bytes = 0;
    for ( size_t i = 0; i < std::min( minBytesForSearchString, bufferSize ); ++i ) {
        bytes = ( bytes << 8 ) | static_cast<uint8_t>( buffer[i] );
    }

    assert( bitStringSize == 48 ); /* this allows us to fixedly load always two bytes (16 bits) */
    for ( size_t i = 0; i < bufferSize; ++i ) {
        bytes = ( bytes << 8 ) | static_cast<uint8_t>( buffer[i] );
        if ( ++i >= bufferSize ) {
            break;
        }
        bytes = ( bytes << 8 ) | static_cast<uint8_t>( buffer[i] );

        for ( size_t j = 0; j < shiftedBitStrings.size(); ++j ) {
            if ( ( bytes & shiftedBitMasks[j] ) == shiftedBitStrings[j] ) {
                return ( i + 1 ) * 8 - bitStringSize - j;
            }
        }
    }

    return std::numeric_limits<size_t>::max();
}
#endif

std::vector<std::pair</* shifted value to compare to */ uint64_t, /* mask */ uint64_t> >
createdShiftedBitStringLUT( uint64_t bitString,
                            uint8_t  bitStringSize )
{
    const auto nWildcardBits = sizeof( uint64_t ) * 8 - bitStringSize;

    std::vector<std::pair<uint64_t, uint64_t> > shiftedBitStrings( nWildcardBits );

    uint64_t shiftedBitString = bitString;
    uint64_t shiftedBitMask = std::numeric_limits<uint64_t>::max() >> nWildcardBits;
    for ( size_t i = 0; i < nWildcardBits; ++i ) {
        shiftedBitStrings[i] = std::make_pair( shiftedBitString, shiftedBitMask );
        shiftedBitString <<= 1;
        shiftedBitMask   <<= 1;
    }

    return shiftedBitStrings;
}


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


inline std::chrono::time_point<std::chrono::high_resolution_clock>
now()
{
    return std::chrono::high_resolution_clock::now();
}


/** @return duration in seconds */
template<typename T0, typename T1>
double
duration( const T0& t0,
          const T1& t1 )
{
    return std::chrono::duration<double>( t1 - t0 ).count();
}


/**
 * @param bitString the lowest bitStringSize bits will be looked for in the buffer
 * @return size_t max if not found else position in buffer
 */
template<uint8_t bitStringSize>
size_t
findBitString( const char* buffer,
               size_t      bufferSize,
               uint64_t    bitString )
{
    const auto& shiftedBitStrings = createdShiftedBitStringLUT( bitString, bitStringSize );

    /* Simply load bytewise even if we could load more (uneven) bits by rounding down.
     * This makes this implementation much less performant in comparison to the "% 8 = 0" version! */
    constexpr auto nBytesToLoadPerIteration = ( sizeof( uint64_t ) * 8 - bitStringSize ) / 8;
    static_assert( nBytesToLoadPerIteration > 0,
                   "Bit string size must be smaller than or equal to 56 bit in order to load bytewise!" );

    /* Initialize buffer window. Note that we can't simply read an uint64_t because of the bit and byte order */
    if ( bufferSize * 8 < bitStringSize ) {
        return std::numeric_limits<size_t>::max();
    }
    uint64_t window = 0;
    const auto nBytesToInitialize = sizeof( uint64_t ) - nBytesToLoadPerIteration;
    for ( size_t i = 0; i < std::min( nBytesToInitialize, bufferSize ); ++i ) {
        window = ( window << 8 ) | static_cast<uint8_t>( buffer[i] );
    }

    for ( size_t i = nBytesToInitialize; i < bufferSize; i += nBytesToLoadPerIteration ) {
        size_t j = 0;
        for ( ; ( j < nBytesToLoadPerIteration ) && ( i + j < bufferSize ); ++j ) {
            window = ( window << 8 ) | static_cast<uint8_t>( buffer[i+j] );
        }

        /* use pre-shifted search bit string values and masks to test for the search string in the larger window */
        for ( size_t k = 0; k < shiftedBitStrings.size(); ++k ) {
            if ( ( window & shiftedBitStrings[k].second ) == shiftedBitStrings[k].first ) {
                return ( i + j ) * 8 - bitStringSize - k;
            }
        }
    }

    return std::numeric_limits<size_t>::max();
}


/** I think this version isn't even correct because magic bytes across buffer boundaries will be overlooked! */
std::vector<size_t>
findBitStrings( const std::string& filename )
{
    const uint64_t bitString = 0x314159265359;
    const uint8_t bitStringSize = 48;

    std::vector<size_t> blockOffsets;

    FILE* file = fopen( filename.c_str(), "rb" );
    std::vector<char> buffer( 2 * 1024 * 1024 );
    size_t nTotalBytesRead = 0;
    while ( true ) {
        #ifdef BENCHMARK
        const auto t0 = now();
        #endif
        const auto nBytesRead = fread( buffer.data(), 1, buffer.size(), file );
        #ifdef BENCHMARK
        const auto t1 = now();
        std::cerr << "Reading " << nBytesRead << " bytes took " << duration( t0, t1 ) << " seconds\n";
        #endif
        if ( nBytesRead == 0 ) {
            break;
        }

        #ifdef BENCHMARK
        const auto t2 = now();
        #endif
        for ( size_t bitpos = 0; bitpos < nBytesRead * 8; ) {
            const auto relpos = findBitString<bitStringSize>( buffer.data() + bitpos / 8, nBytesRead - bitpos / 8, bitString );
            if ( relpos == std::numeric_limits<size_t>::max() ) {
                break;
            }
            bitpos += relpos;
            blockOffsets.push_back( nTotalBytesRead * 8 + bitpos );
            bitpos += bitStringSize;
        }
        nTotalBytesRead += nBytesRead;
        #ifdef BENCHMARK
        const auto t3 = now();

        std::cerr << "Searching bit string took " << duration( t2, t3 ) << " seconds\n";
        #endif
    }

    return blockOffsets;
}

/** use BitReader.read instead of the pre-shifted table trick */
std::vector<size_t>
findBitStrings2( const std::string& filename )
{
    const uint64_t bitString = 0x314159265359;
    const uint8_t bitStringSize = 48;

    std::vector<size_t> blockOffsets;

    BitReader bitReader( filename );

    uint64_t bytes = bitReader.read( bitStringSize - 1 );
    while ( true ) {
        bytes = ( ( bytes << 1 ) | bitReader.read( 1 ) ) & 0xFFFFFFFFFFFF;
        if ( bitReader.eof() ) {
            break;
        }

        if ( bytes == bitString ) {
            blockOffsets.push_back( bitReader.tell() - bitStringSize );
        }
    }

    return blockOffsets;
}

/** always get one more bit but avoid slow BitReader.read calls */
std::vector<size_t>
findBitStrings3( const std::string& filename )
{
    const uint64_t bitString = 0x314159265359;
    const uint8_t bitStringSize = 48;

    std::vector<size_t> blockOffsets;

    FILE* file = fopen( filename.c_str(), "rb" );
    std::vector<char> buffer( 2 * 1024 * 1024 );
    size_t nTotalBytesRead = 0;
    uint64_t window = 0;
    while ( true ) {
        #ifdef BENCHMARK
        const auto t0 = now();
        #endif
        const auto nBytesRead = fread( buffer.data(), 1, buffer.size(), file );
        #ifdef BENCHMARK
        const auto t1 = now();
        std::cerr << "Reading " << nBytesRead << " bytes took " << duration( t0, t1 ) << " seconds\n";
        #endif
        if ( nBytesRead == 0 ) {
            break;
        }

        #ifdef BENCHMARK
        const auto t2 = now();
        #endif
        for ( size_t i = 0; i < nBytesRead; ++i ) {
            auto byte = static_cast<uint8_t>( buffer[i] );
            for ( int j = 0; j < 8; ++j ) {
                const auto bit = ( byte >> ( 7 - j ) ) & 1U;
                window <<= 1;
                window |= bit;
                window &= 0xFFFFFFFFFFFF;
                if ( ( nTotalBytesRead + i ) * 8 + j + 1 < bitStringSize ) {
                    continue;
                }

                if ( window == bitString ) {
                    blockOffsets.push_back( nTotalBytesRead + i + j - bitStringSize );
                }
            }
        }

        nTotalBytesRead += nBytesRead;
        #ifdef BENCHMARK
        const auto t3 = now();
        std::cerr << "Searching bit string took " << duration( t2, t3 ) << " seconds\n";
        #endif
    }

    return blockOffsets;
}


int main( int argc, char** argv )
{
    if ( argc < 2 ) {
        std::cerr << "A bzip2 file name to decompress must be specified!\n";
        return 1;
    }
    const std::string filename ( argv[1] );

    /* comments contain tests on firefox-66.0.5.tar.bz2 */
    const auto blockOffsets = findBitStrings( filename ); // ~520ms
    //const auto blockOffsets = findBitStrings2( filename ); // ~9.5s
    //const auto blockOffsets = findBitStrings3( filename ); // ~520ms
    /* lookup table and manual minimal bit reader are virtually equally fast
     * probably because the encrypted SSD is the limited factor
     * Need to benchmark on buffer -> simply read whole file into buffer and benchmark inside C++
     * => I don't have statistical means (yet) but it feels like the non LUT version is a tad slower
     * Reading 2097152 bytes took 0.000407302 seconds
     * Searching bit string took 0.0163392 seconds
     * => searching is roughly 4x slower, so multithreading on 4 threads should make it equally fast,
     *    which then makes double-buffering a viable option for a total speedup of hopefully 8x!
     */

    std::cerr << "Block offsets  :\n";
    for ( const auto& offset : blockOffsets ) {
        std::cerr << offset / 8 << " B " << offset % 8 << " b\n";
    }

    return 0;
}
