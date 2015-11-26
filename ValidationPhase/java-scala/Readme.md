####There are five cases to consider when evaluation a read operation for each partition:

i.	There are no updates for the specific partition
Evaluate: Against the initial state

ii.	The Read operation occurs before of all the updates 
Evaluate: Against the initial state

iii.	The Read operation overlaps with at least one update
Evaluate 1: Against the values for the overlapping intervals
Evaluate 2: Also against the last interval on the left of the Read Interval
Evaluate 3: Check if Read low is before the lowest update then use initial state

iv.	The Read operation occurs after all the updates
Evaluate: Find overlapping update intervals with the last interval of the tree

v.	There are updates but the Read operation does not overlap with any of them
Evaluate: find the closest update to that read operation:
(update.high < read.low is the minimum OR closest update on the left)
