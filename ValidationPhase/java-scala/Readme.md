####There are five cases to consider when evaluation a read operation for each partition:

i.	There are no updates for the specific partition <br>
Evaluate: Against the initial state <br>

ii.	The Read operation occurs before of all the updates <br> 
Evaluate: Against the initial state <br>

iii.	The Read operation overlaps with at least one update <br>
Evaluate 1: Against the values for the overlapping intervals <br>
Evaluate 2: Also against the last interval on the left of the Read Interval <br>
Evaluate 3: Check if Read low is before the lowest update then use initial state <br>

iv.	The Read operation occurs after all the updates <br>
Evaluate: Find overlapping update intervals with the last interval of the tree <br>

v.	There are updates but the Read operation does not overlap with any of them <br>
Evaluate: find the closest update to that read operation: <br>
(update.high < read.low is the minimum OR closest update on the left) <br>
