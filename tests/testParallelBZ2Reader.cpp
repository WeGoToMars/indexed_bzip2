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
 * head -c $(( 64*1024*1024 )) /dev/urandom > decoded-sample
 * lbzip2 -k -c decoded-sample > encoded-sample.bz2
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


void
require( bool        condition,
         std::string conditionString,
         int         line )
{
    ++gnTests;
    if ( !condition ) {
        ++gnTestErrors;
        std::cerr << "[FAIL on line " << line << "] " << conditionString << "\n";
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
#define REQUIRE( condition ) require( condition, #condition, __LINE__ )


void
testSimpleOpenAndClose()
{
    const auto t0 = std::chrono::high_resolution_clock::now();
    {
        ParallelBZ2Reader encodedFile( encodedTestFilePath );
    }
    const auto t1 = std::chrono::high_resolution_clock::now();
    const auto dt = std::chrono::duration_cast<std::chrono::duration<double> >( t1 - t0 ).count();
    REQUIRE( dt < 1 );
}


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
            std::cerr << "Read " << nBytesToRead << "B\n";
            std::vector<char> decodedBuffer( nBytesToRead, 111 );
            std::vector<char> encodedBuffer( nBytesToRead, 222 );

            /* Why doesn't the ifstream has a similar return specifying the number of read bytes?! */
            decodedFile.read( decodedBuffer.data(), nBytesToRead );
            const auto nBytesRead = encodedFile.read( -1, encodedBuffer.data(), nBytesToRead );

            /* Encountering eof during read also sets fail bit meaning tellg will return -1! */
            if ( !decodedFile.eof() ) {
                REQUIRE_EQUAL( static_cast<ssize_t>( decodedFile.tellg() ),
                               static_cast<ssize_t>( encodedFile.tell() ) );
            }
            REQUIRE_EQUAL( decodedFile.eof(), encodedFile.eof() );
            /* Avoid REQUIRE_EQAL in order to avoid printing huge binary buffers out. */
            int equalElements = 0;
            for ( size_t i = 0; i < decodedBuffer.size(); ++i ) {
                if ( decodedBuffer[i] == encodedBuffer[i] ) {
                    ++equalElements;
                }
            }
            REQUIRE_EQUAL( equalElements, nBytesRead );
        };

    /* Try some subsequent small reads. */
    read( 1 );
    read( 0 );
    read( 1 );
    read( 2 );
    read( 10 );
    read( 100 );
    read( 256 );

    /* Try some subsequent reads over bz2 block boundaries. */
    read( 5*1024*1024 );
    read( 7*1024*1024 );
    read( 1024 );

    /* Try reading over the end of the file. */
    read( 1024*1024*1024 );
}


int
main( void )
{
    testSimpleOpenAndClose();

    testDecodingBz2ForFirstTime();
    /* Sequential Reads on Unknown File */

    /* Seek Forwards on Unknown File */

    /* Seek Backwards on Unknown File */

    std::cout << "Tests successful: " << ( gnTests - gnTestErrors ) << " / " << gnTests << "\n";

    return gnTestErrors;
}
