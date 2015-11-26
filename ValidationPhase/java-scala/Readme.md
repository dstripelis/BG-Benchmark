###There are five cases to consider when evaluation a read operation for each partition:

1.	<strong> There are no updates for the specific partition </strong> <br>
Evaluate: Against the initial state <br>

2.	<strong> The Read operation occurs before of all the updates </strong> <br> 
Evaluate: Against the initial state <br>

3.	<strong> The Read operation overlaps with at least one update </strong> <br>
Evaluate 1: Against the values for the overlapping intervals <br>
Evaluate 2: Also against the last interval on the left of the Read Interval <br>
Evaluate 3: Check if Read low is before the lowest update then use initial state <br>

4.	<strong> The Read operation occurs after all the updates </strong> <br>
Evaluate: Find overlapping update intervals with the last interval of the tree <br>

5.	<strong> There are updates but the Read operation does not overlap with any of them </strong> <br>
Evaluate: find the closest update to that read operation: <br>
(update.high < read.low is the minimum OR closest update on the left) <br>
