Storage Design 

## Introduction


## Storage Layout

The disk storage of a PicPoc database can be viewed as a square matrix of files.
Each file is composed of a series of containers, each containing a number of images.
The container is the unit of I/O.  That is, images are read/written in the unit of
whole containers.  The storage is so designed to assist a global random shuffle of
a huge dataset.

The following parameters have to be chosen:
1. Matrix size M.
2. File size FS.
3. Maximal container size CS.

The parameters are chosen with the following constraints:
- M * M * FS ~ size of dataset
- CS should be > 100MB to achieve sequential I/O throughput.
- CS * M * 2 = buffer overhead.
- FS should be small enough to be contained in main memory.

For example, the ImageNet dataset has the size of 1.1TB.  The following parameters
are chosen:

M = 20
CS = 100MB
FS = 1.1TB/20/20 = 2.8GB

Buffer overhead is at the level of 20*100MB*2 = 4GB.


## Random Shuffle of Whole DataSet

Random shuffle of whole dataset is achieved by the following procedure:

- Transpose the file matrix.
- Load each file into main memory and random shuffle it.
- Read and write to another dataset.


