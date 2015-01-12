simple-mongo-benchmark
======================

Simple MongoDB benchmark in python. Load data, then run test queries. Original 
use was to model a simple time series / internet of things, but you can replace 
the data load to load any data, and the queries to test anything.

    sudo pip install pymongo numpy
    git clone https://github.com/henrikingo/mongo-timeseries-benchmark.git
    cd mongo-timeseries-benchmark
    python timeSeriesTest.py


See beginning of timeSeriesTest.py for parameters.



Note: To keep up with improved performance in MongoDB 2.8, I've had to move to
using the python multiprocessing module to drive multiple parallel clients.
With this change the original mongodump approach has stopped working. The 
mongodump approach was becoming a bottleneck anyway, so using batch (array) 
inserts is the way forward.
