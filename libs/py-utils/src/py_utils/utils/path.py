import os
import shutil
from pathlib import Path

def join_paths(*paths):
    """
    Join multiple paths into one.
    
    :param paths: Paths to be joined.
    :return: Joined path as a string.
    """
    return os.path.join(*paths)

def path_exists(path):
    """
    Check if a path exists.
    
    :param path: Path to be checked.
    :return: True if the path exists, False otherwise.
    """
    return os.path.exists(path)

def create_directory(path):
    """
    Create a directory and any necessary parent directories.
    
    :param path: Path of the directory to be created.
    """
    os.makedirs(path, exist_ok=True)

def remove_directory(path):
    """
    Remove a directory and all its contents.
    
    :param path: Path of the directory to be removed.
    """
    shutil.rmtree(path)

def list_files_in_directory(path):
    """
    List all files in a directory.
    
    :param path: Path of the directory.
    :return: List of file names.
    """
    return [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]

def list_directories_in_directory(path):
    """
    List all directories in a directory.
    
    :param path: Path of the directory.
    :return: List of directory names.
    """
    return [d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))]

def copy_file(src, dst):
    """
    Copy a file from source to destination.
    
    :param src: Source file path.
    :param dst: Destination file path.
    """
    shutil.copy(src, dst)

def move_file(src, dst):
    """
    Move a file from source to destination.
    
    :param src: Source file path.
    :param dst: Destination file path.
    """
    shutil.move(src, dst)

def delete_file(path):
    """
    Delete a file.
    
    :param path: Path of the file to be deleted.
    """
    os.remove(path)

def get_file_size(path):
    """
    Get the size of a file in bytes.
    
    :param path: Path of the file.
    :return: Size of the file in bytes.
    """
    return os.path.getsize(path)

def get_file_extension(path):
    """
    Get the extension of a file.
    
    :param path: Path of the file.
    :return: File extension as a string.
    """
    return os.path.splitext(path)[1]

def read_file(path):
    """
    Read the contents of a file.
    
    :param path: Path of the file to be read.
    :return: Contents of the file as a string.
    """
    with open(path, 'r') as file:
        return file.read()

def write_file(path, content):
    """
    Write content to a file.
    
    :param path: Path of the file to be written.
    :param content: Content to be written to the file.
    """
    with open(path, 'w') as file:
        file.write(content)

def append_to_file(path, content):
    """
    Append content to a file.
    
    :param path: Path of the file to be appended.
    :param content: Content to be appended to the file.
    """
    with open(path, 'a') as file:
        file.write(content)

def get_absolute_path(path):
    """
    Get the absolute path of a given path.
    
    :param path: Path to be converted to absolute path.
    :return: Absolute path as a string.
    """
    return os.path.abspath(path)

def normalize_path(path):
    """
    Normalize a path, eliminating double slashes, etc.
    
    :param path: Path to be normalized.
    :return: Normalized path as a string.
    """
    return os.path.normpath(path)

def split_path(path):
    """
    Split a path into a pair (head, tail) where tail is the last pathname component and head is everything leading up to that.
    
    :param path: Path to be split.
    :return: A tuple (head, tail).
    """
    return os.path.split(path)

def get_basename(path):
    """
    Get the base name of a path.
    
    :param path: Path to get the base name of.
    :return: Base name of the path.
    """
    return os.path.basename(path)

def get_dirname(path):
    """
    Get the directory name of a path.
    
    :param path: Path to get the directory name of.
    :return: Directory name of the path.
    """
    return os.path.dirname(path)