# Scala-Spark-Stellar-Classification

This work is based on the Stellar Classification Dataset - SDSS17 (https://www.kaggle.com/datasets/fedesoriano/stellar-classification-dataset-sdss17). This dataset consists of 100000 space observations obtained through the SDSS (Sloan Digital Sky Survey). Each observation is described by 17 features (columns) plus 1 column for the class which is then used for the classification task (star, galaxy or quasar). 


In order to see some differences in the execution time, we train and evaluate a sequential Decision Tree and a parallel Random Forest classifier (ensemble of trees). The latter comes handy since it's already defined in the *spark.ml* library (the MLlib DataFrame-based API) for multiclass classification, using both continuous and categorical features. For the training and testing phases we split the dataset following a 80%/20% logic. By running the code on local machines we see already a considerable difference between the two models under execution times, with the first being almost 20 times slower than its parallel counterpart. Nonetheless both models reach an acceptable level of accuracy of 0.97.


Since we wanted to test our models on a proper distributed and parallel environment we then deployed this project on Google Cloud Platform, evaluating it using various configurations of cluster of nodes.
