# NNfcast
This project is a neural network forecaster for equities such as the SP 500.  The code collected data from tens of economic data series and stored them in a relational database.  Various utilities were used to validate data integrity.  Finally, early attempts to perform forecasting with a neural network were developed.  A number of important next steps involved the prevention of overfitting and other methods to assess the integrity of the model.  

Please note that this code was never intended to be shared.  The examples here are not commented for team collaboration, may have various platform dependencies and were intended to be part of a larger system so may not run independently.  They are only provided as example works.  

## Included Samples

* loadeod.py - This was an example of code used to load data from a remote server into MySQL for later processing
* normalize.py - Much of the data in this system required normalization before processing.  This program performed that funciton.
* idxdpnn.py - This was a neural network back propagation utility from another coder leveraged for this work. 
* idxnn.py - This program organized and prepped the training of the neural network.
* idxutils.py - This is a series of utilies used by this project.
* quotes.py - This is a program that was used to get quotes from a web service for this project.
* smooth.py - Some of the time series for this project required smoothing.  This utility performed that function.
