#ifndef ranksel_macros_h
#define ranksel_macros_h

#define ONES_STEP_4 ( 0x1111111111111111ULL )
#define ONES_STEP_8 ( 0x0101010101010101ULL )
#define MSBS_STEP_8 ( 0x80ULL * ONES_STEP_8 )
#define EASY_LEQ_STEP_8_MSBS(x,y) ( ( ( ( (y) | MSBS_STEP_8 ) - ( x ) ) ) & MSBS_STEP_8 )


#endif
