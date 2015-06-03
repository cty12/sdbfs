# SDBFS - A **S**imple **D**atabase **F**ile **S**ystem 

## About the Project

The project contains two files. `passthrough.py` is the implementation of a pass-through file system using Fusepy. It aims at aiding a beginner to get used to Fuse and Fusepy as soon as possible. You can refer to [this blog post](http://www.stavros.io/posts/python-fuse-filesystem/) for further help.

`myfs.py` is my implementation of a very simple database file system. It requires sqlite3 and its python module as well as Fuse and Fusepy. If you use Ubuntu or Debian you can easily acquire these dependencies via apt-get and pip. 

## Known Issues

The file system only supports one layer of files / directories. And this is why it is called 'very simple'.

## License

MIT License. 
