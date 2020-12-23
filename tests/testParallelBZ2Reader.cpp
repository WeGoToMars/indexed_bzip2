#include <cassert>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <stdexcept>

#include <BitStringFinder.hpp>
#include <common.hpp>
#include <ParallelBZ2Reader.hpp>


namespace
{
int gnTests = 0;
int gnTestErrors = 0;

/**
 * head -c $(( 100*1024*1024 )) /dev/urandom > decoded-sample
 * pbzip2 -k -c decoded-sample > encoded-sample.bz2
 */
const std::string encodedTestFilePath = "encoded-sample.bz2";
const std::string decodedTestFilePath = "decoded-sample";


template<typename T>
void
requireEqual( const T& a, const T& b, int line )
{
    ++gnTests;
    if ( a != b ) {
        ++gnTestErrors;
        std::cerr << "[FAIL on line " << line << "] " << a << " != " << b << "\n";
    }
}


std::ios_base::seekdir
toSeekdir( int origin )
{
    switch ( origin )
    {
    case SEEK_SET: return std::ios_base::beg;
    case SEEK_CUR: return std::ios_base::cur;
    case SEEK_END: return std::ios_base::end;
    }

    throw std::invalid_argument( "Unknown origin" );
}
}


#define REQUIRE_EQUAL( a, b ) requireEqual( a, b, __LINE__ )


/**
 * Tests are in such a way that seeking and reading are mirrored on the ParallelBZ2Reader file and the decoded file.
 * Then all read results can be checked against each other. Same for the result of tell.
 */
void
testDecodingBz2ForFirstTime()
{
    std::ifstream decodedFile( decodedTestFilePath );
    ParallelBZ2Reader encodedFile( encodedTestFilePath );

    const auto seek =
        [&]( long long int offset,
             int           origin = SEEK_SET )
        {
            decodedFile.seekg( offset, toSeekdir( origin ) );
            const auto newSeekPosDecoded = static_cast<ssize_t>( decodedFile.tellg() );
            const auto newSeekPosEncoded = static_cast<ssize_t>( encodedFile.seek( offset, origin ) );
            REQUIRE_EQUAL( newSeekPosDecoded, newSeekPosEncoded );
            REQUIRE_EQUAL( static_cast<ssize_t>( decodedFile.tellg() ), static_cast<ssize_t>( encodedFile.tell() ) );
            REQUIRE_EQUAL( decodedFile.eof(), encodedFile.eof() );
        };

    const auto read =
        [&]( size_t nBytesToRead )
        {
            std::vector<char> decodedBuffer( nBytesToRead, 111 );
            std::vector<char> encodedBuffer( nBytesToRead, 333 );
            decodedFile.read( decodedBuffer.data(), nBytesToRead );
            encodedFile.read( -1, encodedBuffer.data(), nBytesToRead );
            REQUIRE_EQUAL( static_cast<ssize_t>( decodedFile.tellg() ), static_cast<ssize_t>( encodedFile.tell() ) );
            REQUIRE_EQUAL( decodedFile.eof(), encodedFile.eof() );
            REQUIRE_EQUAL( decodedBuffer, encodedBuffer );
        };

    /* Try some subsequent reads over bz2 block boundaries */

}


int
main( void )
{
    testDecodingBz2ForFirstTime();
    /* Sequential Reads on Unknown File */

    /* Seek Forwards on Unknown File */

    /* Seek Backwards on Unknown File */

    std::cout << "Tests successful: " << ( gnTests - gnTestErrors ) << " / " << gnTests << "\n";

    return gnTestErrors;
}
