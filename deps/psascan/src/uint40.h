/*============================================
 # Source code: https://www.cs.helsinki.fi/group/pads/pSAscan.html
 #
 # Paper: Parallel external memory suffix sorting
 # Authors: Juha Kärkkäinen, Dominik Kempa, Simon J. Puglisi.
 # In Proc. CPM 2015, pp. 329–342.
============================================*/

#ifndef __UINT40_H_INCLUDED
#define __UINT40_H_INCLUDED

#include <inttypes.h>
#include <stdint.h>
#include <cassert>
#include <iostream>
#include <limits>
#include <unistd.h>


class uint40
{
private:
    uint32_t    low;
    uint8_t     high;

public:
    inline uint40()
    {
    }

    inline uint40(uint32_t l, uint8_t h)
        : low(l), high(h)
    {
    }

    inline uint40(const uint40& a)
        : low(a.low), high(a.high)
    {
    }

    inline uint40(const int& a)
        : low(a), high(0)
    {
    }

    inline uint40(const unsigned int& a)
      : low(a), high(0)
    {
    }

    inline uint40(const uint64_t& a)
        : low(a & 0xFFFFFFFF), high((a >> 32) & 0xFF)
    {
        assert( a <= 0xFFFFFFFFFFLU );
    }

    inline uint40(const long& a)
      : low(a & 0xFFFFFFFFL), high((a >> 32) & 0xFF) {
      assert( a <= 0xFFFFFFFFFFL );
    }

    inline uint64_t ull() const {
        return ((uint64_t)high) << 32 | (uint64_t)low;
    }

    inline long ll() const
    {
        return (long)ull();
    }

    inline operator uint64_t() const
    {
        return ull();
    }

//    inline uint64_t u64() const
//    {
//        return ((uint64_t)high) << 32 | (uint64_t)low;
//    }

    inline uint40& operator++ ()
    {
        if (low == std::numeric_limits<uint32_t>::max())
            ++high, low = 0;
        else
            ++low;
        return *this;
    }

    inline uint40& operator-- ()
    {
        if (low == 0)
            --high, low = std::numeric_limits<uint32_t>::max();
        else
            --low;
        return *this;
    }

    inline uint40& operator+= (const uint40& b)
    {
        uint64_t add = (uint64_t)low + b.low;  // BUGFIX
        low = add & 0xFFFFFFFF;
        high += b.high + ((add >> 32) & 0xFF);
        return *this;
    }

    inline bool operator== (const uint40& b) const
    {
        return (low == b.low) && (high == b.high);
    }

    inline bool operator!= (const uint40& b) const
    {
        return (low != b.low) || (high != b.high);
    }

    inline bool operator< (const uint40& b) const
    {
        return (high < b.high) || (high == b.high && low < b.low);
    }

    inline bool operator<= (const uint40& b) const
    {
        return (high < b.high) || (high == b.high && low <= b.low);
    }

    inline bool operator> (const uint40& b) const
    {
        return (high > b.high) || (high == b.high && low > b.low);
    }

    inline bool operator>= (const uint40& b) const
    {
        return (high > b.high) || (high == b.high && low >= b.low);
    }

    friend std::ostream& operator<< (std::ostream& os, const uint40& a)
    {
        return os << a.ull();
    }

} __attribute__((packed));

namespace std {

template<>
class numeric_limits<uint40> {
public:
    static uint40 min() { return uint40(std::numeric_limits<uint32_t>::min(),
                                        std::numeric_limits<uint8_t>::min()); }

    static uint40 max() { return uint40(std::numeric_limits<uint32_t>::max(),
                                        std::numeric_limits<uint8_t>::max()); }
};

}

#endif  // __UINT40_H_INCLUDED
