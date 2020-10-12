#include "bzip2.hpp"

#include <array>
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


template<uint8_t bitStringSize>
constexpr auto
createdShiftedBitStringLUTArray( uint64_t bitString )
{
    constexpr auto nWildcardBits = sizeof( uint64_t ) * 8 - bitStringSize;
    using ShiftedLUTTable = std::array<std::pair</* shifted value to compare to */ uint64_t, /* mask */ uint64_t>,
                                       nWildcardBits>;
    ShiftedLUTTable shiftedBitStrings;

    uint64_t shiftedBitString = bitString;
    uint64_t shiftedBitMask = std::numeric_limits<uint64_t>::max() >> nWildcardBits;
    for ( size_t i = 0; i < nWildcardBits; ++i ) {
        shiftedBitStrings[i] = std::make_pair( shiftedBitString, shiftedBitMask );
        shiftedBitString <<= 1;
        shiftedBitMask   <<= 1;
    }

    return shiftedBitStrings;
}


auto
createdShiftedBitStringLUT( uint64_t bitString,
                            uint8_t  bitStringSize )
{
    const auto nWildcardBits = sizeof( uint64_t ) * 8 - bitStringSize;
    using ShiftedLUTTable = std::vector<std::pair</* shifted value to compare to */ uint64_t, /* mask */ uint64_t> >;
    ShiftedLUTTable shiftedBitStrings( nWildcardBits );

    uint64_t shiftedBitString = bitString;
    uint64_t shiftedBitMask = std::numeric_limits<uint64_t>::max() >> nWildcardBits;
    for ( size_t i = 0; i < nWildcardBits; ++i ) {
        shiftedBitStrings[i] = std::make_pair( shiftedBitString, shiftedBitMask );
        shiftedBitString <<= 1;
        shiftedBitMask   <<= 1;
        assert( ( shiftedBitString & shiftedBitMask ) == shiftedBitString );
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
    #if 1
    const auto shiftedBitStrings = createdShiftedBitStringLUT( bitString, bitStringSize );
    #elif 0
    /* Not much of a difference. If anything, it feels like 1% slower */
    const auto shiftedBitStrings = createdShiftedBitStringLUTArray<bitStringSize>( bitString );
    #else
    /* This version actually takes 50% longer for some reason! */
    constexpr std::array<std::pair<uint64_t, uint64_t>, 17> shiftedBitStrings = {
        std::make_pair( 0x0000'3141'5926'5359ULL, 0x0000'ffff'ffff'ffffULL ),
        std::make_pair( 0x0000'6282'b24c'a6b2ULL, 0x0001'ffff'ffff'fffeULL ),
        std::make_pair( 0x0000'c505'6499'4d64ULL, 0x0003'ffff'ffff'fffcULL ),
        std::make_pair( 0x0001'8a0a'c932'9ac8ULL, 0x0007'ffff'ffff'fff8ULL ),
        std::make_pair( 0x0003'1415'9265'3590ULL, 0x000f'ffff'ffff'fff0ULL ),
        std::make_pair( 0x0006'282b'24ca'6b20ULL, 0x001f'ffff'ffff'ffe0ULL ),
        std::make_pair( 0x000c'5056'4994'd640ULL, 0x003f'ffff'ffff'ffc0ULL ),
        std::make_pair( 0x0018'a0ac'9329'ac80ULL, 0x007f'ffff'ffff'ff80ULL ),
        std::make_pair( 0x0031'4159'2653'5900ULL, 0x00ff'ffff'ffff'ff00ULL ),
        std::make_pair( 0x0062'82b2'4ca6'b200ULL, 0x01ff'ffff'ffff'fe00ULL ),
        std::make_pair( 0x00c5'0564'994d'6400ULL, 0x03ff'ffff'ffff'fc00ULL ),
        std::make_pair( 0x018a'0ac9'329a'c800ULL, 0x07ff'ffff'ffff'f800ULL ),
        std::make_pair( 0x0314'1592'6535'9000ULL, 0x0fff'ffff'ffff'f000ULL ),
        std::make_pair( 0x0628'2b24'ca6b'2000ULL, 0x1fff'ffff'ffff'e000ULL ),
        std::make_pair( 0x0c50'5649'94d6'4000ULL, 0x3fff'ffff'ffff'c000ULL ),
        std::make_pair( 0x18a0'ac93'29ac'8000ULL, 0x7fff'ffff'ffff'8000ULL ),
        std::make_pair( 0x3141'5926'5359'0000ULL, 0xffff'ffff'ffff'0000ULL )
    };
    #endif

    #if 0
    std::cerr << "Shifted Bit Strings:\n";
    for ( const auto [shifted, mask] : shiftedBitStrings ) {
        std::cerr << "0x" << std::hex << shifted << " 0x" << mask << "\n";
    }
    #endif

    /* Simply load bytewise even if we could load more (uneven) bits by rounding down.
     * This makes this implementation much less performant in comparison to the "% 8 = 0" version! */
    constexpr auto nBytesToLoadPerIteration = ( sizeof( uint64_t ) * CHAR_BIT - bitStringSize ) / CHAR_BIT;
    static_assert( nBytesToLoadPerIteration > 0,
                   "Bit string size must be smaller than or equal to 56 bit in order to load bytewise!" );

    /* Initialize buffer window. Note that we can't simply read an uint64_t because of the bit and byte order */
    if ( bufferSize * CHAR_BIT < bitStringSize ) {
        return std::numeric_limits<size_t>::max();
    }
    //std::cerr << "nBytesToLoadPerIteration: " << nBytesToLoadPerIteration << "\n"; // 2
    uint64_t window = 0;
    const auto nBytesToInitialize = sizeof( uint64_t ) - nBytesToLoadPerIteration;
    for ( size_t i = 0; i < std::min( nBytesToInitialize, bufferSize ); ++i ) {
        window = ( window << CHAR_BIT ) | static_cast<uint8_t>( buffer[i] );
    }

    for ( size_t i = std::min( nBytesToInitialize, bufferSize ); i < bufferSize; i += nBytesToLoadPerIteration ) {
        size_t j = 0;
        for ( ; ( j < nBytesToLoadPerIteration ) && ( i + j < bufferSize ); ++j ) {
            window = ( window << CHAR_BIT ) | static_cast<uint8_t>( buffer[i+j] );
        }

        /* use pre-shifted search bit string values and masks to test for the search string in the larger window */
        #define LOOP_METHOD 0
        #if LOOP_METHOD == 0
        /* AMD Ryzen 9 3900X clang++ 10.0.0-4ubuntu1       -O3 -DNDEBUG               : 1.7s */
        /* AMD Ryzen 9 3900X clang++ 10.0.0-4ubuntu1       -O3 -DNDEBUG -march=native : 1.8s */
        /* AMD Ryzen 9 3900X g++     10.2.0-5ubuntu1~20.04 -O3 -DNDEBUG               : 2.8s */
        /* AMD Ryzen 9 3900X g++     10.2.0-5ubuntu1~20.04 -O3 -DNDEBUG -march=native : 3.0s */
        size_t k = 0;
        for ( const auto& [shifted, mask] : shiftedBitStrings ) {
            if ( ( window & mask ) == shifted ) {
                return ( i + j ) * CHAR_BIT - bitStringSize - k;
            }
            ++k;
        }
        #elif LOOP_METHOD == 1
        /* AMD Ryzen 9 3900X clang++ 10.0.0-4ubuntu1       -O3 -DNDEBUG: 2.0s */
        /* AMD Ryzen 9 3900X g++     10.2.0-5ubuntu1~20.04 -O3 -DNDEBUG: 3.3s */
        for ( size_t k = 0; k < shiftedBitStrings.size(); ++k ) {
            const auto& [shifted, mask] = shiftedBitStrings[k];
            if ( ( window & mask ) == shifted ) {
                return ( i + j ) * CHAR_BIT - bitStringSize - k;
            }
        }
        #elif LOOP_METHOD == 2
        /* AMD Ryzen 9 3900X clang++ 10.0.0-4ubuntu1       -O3 -DNDEBUG               : 2.0s */
        /* AMD Ryzen 9 3900X clang++ 10.0.0-4ubuntu1       -O3 -DNDEBUG -march=native : 2.1s */
        /* AMD Ryzen 9 3900X g++     10.2.0-5ubuntu1~20.04 -O3 -DNDEBUG               : 3.3s */
        /* AMD Ryzen 9 3900X g++     10.2.0-5ubuntu1~20.04 -O3 -DNDEBUG -march=native : 3.6s */
        for ( size_t k = 0; k < shiftedBitStrings.size(); ++k ) {
            if ( ( window & shiftedBitStrings[k].second ) == shiftedBitStrings[k].first ) {
                return ( i + j ) * CHAR_BIT - bitStringSize - k;
            }
        }
        #elif LOOP_METHOD == 3
        /* AMD Ryzen 9 3900X clang++ 10.0.0-4ubuntu1 -O3 -DNDEBUG : 2.0s */
        const auto match = std::find_if(
            shiftedBitStrings.begin(), shiftedBitStrings.end(),
            [window] ( const auto& pair ) { return ( window & pair.second ) == pair.first; }
        );
        if ( match != shiftedBitStrings.end() ) {
            return ( i + j ) * CHAR_BIT - bitStringSize - ( match - shiftedBitStrings.begin() );
        }
        #endif
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
    const auto movingBytesToKeep = ceilDiv( bitStringSize, CHAR_BIT ) * CHAR_BIT; // 6
    std::vector<char> buffer( 2 * 1024 * 1024 + movingBytesToKeep ); // for performance testing
    //std::vector<char> buffer( 53 ); // for bug testing with bit strings accross buffer boundaries
    size_t nTotalBytesRead = 0;
    while ( true ) {
        size_t nBytesRead = 0;
        if ( nTotalBytesRead == 0 ) {
            nBytesRead = fread( buffer.data(), 1, buffer.size(), file );
            buffer.resize( nBytesRead );
        } else {
            std::memmove( buffer.data(), buffer.data() + buffer.size() - movingBytesToKeep, movingBytesToKeep );
            nBytesRead = fread( buffer.data() + movingBytesToKeep, 1, buffer.size() - movingBytesToKeep, file );
            buffer.resize( movingBytesToKeep + nBytesRead );
        }
        if ( nBytesRead == 0 ) {
            break;
        }

        for ( size_t bitpos = 0; bitpos < nBytesRead * CHAR_BIT; ) {
            const auto byteOffset = bitpos / CHAR_BIT; // round down because we can't give bit precision
            const auto relpos = findBitString<bitStringSize>( buffer.data() + byteOffset,
                                                              buffer.size() - byteOffset,
                                                              bitString );
            if ( relpos == std::numeric_limits<size_t>::max() ) {
                break;
            }
            bitpos = byteOffset * CHAR_BIT + relpos;
            const auto foundOffset = ( nTotalBytesRead > movingBytesToKeep
                                       ? nTotalBytesRead - movingBytesToKeep
                                       : nTotalBytesRead ) * CHAR_BIT + bitpos;
            if ( blockOffsets.empty() || ( blockOffsets.back() != foundOffset ) ) {
                blockOffsets.push_back( foundOffset );
            }
            bitpos += bitStringSize;
        }
        nTotalBytesRead += nBytesRead;
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
        const auto nBytesRead = fread( buffer.data(), 1, buffer.size(), file );
        if ( nBytesRead == 0 ) {
            break;
        }

        for ( size_t i = 0; i < nBytesRead; ++i ) {
            const auto byte = static_cast<uint8_t>( buffer[i] );
            for ( int j = 0; j < CHAR_BIT; ++j ) {
                const auto bit = ( byte >> ( CHAR_BIT - 1 - j ) ) & 1U;
                window <<= 1;
                window |= bit;
                if ( ( nTotalBytesRead + i ) * CHAR_BIT + j < bitStringSize ) {
                    continue;
                }

                if ( ( window & 0xFFFF'FFFF'FFFFULL ) == bitString ) {
                    /* Dunno why the + 1 is necessary but it works (tm) */
                    blockOffsets.push_back( ( nTotalBytesRead + i ) * CHAR_BIT + j + 1 - bitStringSize );
                }
            }
        }

        nTotalBytesRead += nBytesRead;
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
    const auto blockOffsets = findBitStrings( filename ); // ~520ms // ~1.7s on /dev/shm with 911MiB large.bz2
    //const auto blockOffsets = findBitStrings2( filename ); // ~9.5s // ~100s on /dev/shm with 911MiB large.bz2
    //const auto blockOffsets = findBitStrings3( filename ); // ~520ms // 6.4s on /dev/shm with 911MiB large.bz2
    /* lookup table and manual minimal bit reader were virtually equally fast
     * probably because the encrypted SSD was the limiting factor -> repeat with /dev/shm
     * => searching is roughly 4x slower, so multithreading on 4 threads should make it equally fast,
     *    which then makes double-buffering a viable option for a total speedup of hopefully 8x!
     */

    BitReader bitReader( filename );
    std::cerr << "Block offsets  :\n";
    for ( const auto offset : blockOffsets ) {
        std::cerr << offset / 8 << " B " << offset % 8 << " b";
        if ( ( offset >= 0 ) && ( offset < bitReader.size() ) ) {
            bitReader.seek( offset );
            const auto magicBytes = bitReader.read( 32 );
            std::cerr << " -> magic bytes: 0x" << std::hex << magicBytes << std::dec;
            if ( magicBytes != 0x3141'5926 ) {
                throw std::logic_error( "Magic Bytes do not match!" );
            }
        }
        std::cerr << "\n";
    }
    std::cerr << "Found " << blockOffsets.size() << " blocks\n";

    return 0;
}
