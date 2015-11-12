# PicPoc: An Image Storage for Deep Learning

Wei Dong (wdong@wdong.org)

# Introduction

PicPoc is an image storage designed for deep learning with large dataset.
Existing solutions usually use general-purpose key-value store, e.g. Caffe with
LMDB, or MXNet with dmlc::RecordIO, and image pre-processing algorithms (decoding,
and augmentation) are typically tightly integrated into the learning framework.
By building a system that specializes image storage and pre-processing, we can
achieve speed/throughput optimization and provide more image-related
functionalities than a framework focusing on learning algorithms does.

The design of PicPoc was motivated by the following observations:
- Deep learning does not require looking up images by ID.
- Rather, randomized input sequencing is important.
- Most of the computatoin happens on GPU, and CPU is under-utilized.
  (When training Caffenet with i7-3770K and Titan X, CPU utilization is around 200%.)
- Data set can be very large.  The ImageNet is > 1TB.  
- Therefore the capacity of traditional HDD is still desirable.
- The learning algorithm is tolerate to minor data loss.

Performance-wise, the goal of PicPoc is to exploit the sequential I/O throughput
of traditional HDD and the under-utilized CPU power so as to achieve an input
throughput that keeps the GPU busy.

For example, the throughput of Caffe with cuDNN when training the Caffenet is
about 800 images/s/GPU.  If the target is to keep 4xGPU busy, the input rate
will have to reach 3200 images/s.  If images are stored as raw pixels of 256x256x3,
that rate translates to 600MB/s, which demands an enterprise-grade SSD.
Measurement show that JPEG encoding provides 4x reduction, while a i7-3770K
CPU is capable of decoding 6500 images/s. Encoding images to JPEG reduces disk
throughput to 150MB/s which can be comfortably provided by two HDDs, and image
decoding will only be using 50% CPU power.  More agressive compression algorithm,
namely MozJPEG, can further reduce data size without sacrificing image quality
and decoding throughput.  (More agressive codecs, like JPEG2000 and WebP, are too
expensive to decode with today's available software.)

Function-wise, PicPoc allows more flexible data handling and mixing.  Currently
implemented features include:
- Efficient global shuffling a dataset.
  The disk layout of PicPoc is specially designed so that shuffling can be
  efficiently realized.
- Mixing multiple dataset at fixed ratio.  For example, some times it is desirable
  to mix samples from different categories with a fixed ratio, e.g. 20% positive
  and 80% negative.  PicPoc's approach to such requirement is to separately
  build datasets for different categories, and use a DataMux layer to mix multiple
  sources with a desired ratio.

# Disk Layout

Each PicPoc dataset is contained in a directory with multiple sub-directories,
each containing a number of files with roughly the same size.  For example:

```
      DIR  FILES
ROOT - 0 - 0, 1, 2, ...
       |
       1 - 0, 1, 2, ...
       |
       2 - 0, 1, 2, ...
```

Each subdirectory is considered as a stream.  When data are appended to the
dataset, they go to the streams in a round-robin fashion.  The data of each
stream is splitted into files, with new files created when the current file
written to reaches certain threshold.  The threshold is set such that
a whole file can be loaded into main memory and shuffled.  Each file is
organized as consecutive containers of roughly the same size, each containing
data of multiple images.  Containers are the disk I/O unit, and are properly
aligned and padded.  Each container is also CRC-checksumed to ensure data
integrity.

# Usage

## Importing datasets.

Currently there are three ways to import images into a dataset.

- load-dir:  imports all images in a directory into a dataset, with the same label.
- load-caffe: imports images from a list, like how Caffe's convert_imageset works.
- load-imagenet: imports ImageNet data, with images of each category stored in a tar file.

## Configuring DataMux.

Warning: config file format is subject to change in the near future.

PicPoc's DataSet class supports an easy to use reading/writing API, but for 
full performance and flexibility, the DataMux class is used.  The user needs to
prepare a configuration file like the one below:

```
path_to_positive_dataset	1	20
path_to_negative_dataset	0	80
```

There can be multiple lines in the configuration file.  Each line contains three fields:
- Path to the dataset.
- Base label for this dataset.  This is added to the label of the dataset to produce the
  actual labels.  The above example assumes that both positive and negative datasets are
  imported with the same label 0.  Alternatively, if the datasets are already properly
  labeled, this column should be set to 0 for all rows.
- Number of image to read from the dataset in each batch.

With the above configuraton, PicPoc reads data in batches of 100, with 20 from positive
examples and 80 from negative ones. 

The data reading API is simple.  The following snippet reads the data in a dead loop.
Each dataset automatically rewinds when the end is reached.

```
picpoc::DataMux mux(path_to_config);
picpoc::Sample sample;
mux.peek(&sample); // does not move cursor
// infer data size with sample
for (;;) {
  mux.read(&sample);
  // do something with sample.label and sample.image
}
```

# Benchmark

TODO
