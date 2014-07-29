'''

   o oo
    x      x       x        x              x
                  o
                 x      x x    x  x x
                       v
                     oo  
x   x    x   x   x  x        x

create all onset chains
onset chains are a series of anchors and grace levels

(TIME0, n)
(TIME0-e, n-1)
(TIME0-e, n-2)
(TIME0+e, n+1)
(TIME0+e, n+2)

ALGORITHM
1) Find local onsets
2) Find global onsets, merge
3) Start at grace level 0, get larger
4) Going down this, we find all grace strings in a gap, multiply by the smallest necessary factor
5) This will be the order, with non_duration_log ALWAYS coming before duration_log
'''