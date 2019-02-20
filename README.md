Events processing - ML style
============================

Trying to find patterns in events, starting from associations, prefix span, FP-growth algorithms, 
throught bio algos - DNA sequencing style, RNN, LSTM, etc...


## Status
Initial FP-Growth is working.
There are some predictions / expectations.
DONE patterns in non-repetitive events/products:
```
A B C
A B 
A D D 
```

## TODO
Looking into more advanced stuff:  
`A B B B B B C D D E`
correlated with:  
`A B B C D E`
in bio-world this correlation would be, like:  
`A B B - - - C D - E`  
`1 1 1 0 0 0 1 1 0 1  => 6`
also 
```
A B B B B B C D D E
A D B C D E
AxB - - - - C D - E  //x-means deleted first D
1 (-1) 1 0 0 0 0 1 1 0 1 => 5 -1 => 4
```
  

## Changelog

### v 0.01 
 - FP-Growth with shopping suggestions
 - FP-Growth initial MLlib examples
 
