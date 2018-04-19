Instructions:

The programm will read cvf files from input directory on hdfs and output pairs of strings from different collections that hash to the same bucket together with Jaccard similarity in output directory.

To run the project use Target/stryga-1.0.jar.

Start hadoop and move input files to input directory on hdfs.
Run stryga-1.0.jar with Tnain main class on hadoop and pass the path to the input and output folders on hdfs as command line arguments.
