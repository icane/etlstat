import pandas as pd

# reads the csv, takes only the first column and creates a set out of it.
A = set(pd.read_csv("/var/git/python/etlstat/etlstat/text/test/c1.csv", index_col=False, header=None)[0])
# same here
B = set(pd.read_csv("/var/git/python/etlstat/etlstat/text/test/c2.csv", index_col=False, header=None)[0])

# test whether every element in B is in A
print(B <= A)

# test whether every element in A is in B
print(B >= A)

# new set with elements from both A and B
print(A | B)

# new set with elements common to A and B
print(A & B)

# new set with elements in A but not in B
print(A - B)

# new set with elements in either A or B but not both
print(A ^ B)
