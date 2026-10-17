from setuptools import setup
import re
from pathlib import Path

# Read version from slick_stream_buffer_multiplexer_py.py
def get_version():
    with open('slick_stream_buffer_multiplexer_py.py', 'r', encoding='utf-8') as f:
        content = f.read()
        match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", content, re.M)
        if match:
            return match.group(1)
        raise RuntimeError("Unable to find version string.")

# Read long description from README
def get_long_description():
    readme_path = Path(__file__).parent / 'README.md'
    if readme_path.exists():
        with open(readme_path, 'r', encoding='utf-8') as f:
            return f.read()
    return ''

setup(
    name='slick-stream-buffer-multiplexer-py',
    version=get_version(),
    description='Lock-free MPMC byte stream multiplexer with C++ interoperability via shared memory',
    long_description=get_long_description(),
    long_description_content_type='text/markdown',
    author='Slick Quant',
    author_email='slickquant@slickquant.com',
    url='https://github.com/SlickQuant/slick-stream-buffer-multiplexer-py',
    project_urls={
        'Bug Tracker': 'https://github.com/SlickQuant/slick-stream-buffer-multiplexer-py/issues',
        'Documentation': 'https://github.com/SlickQuant/slick-stream-buffer-multiplexer-py#readme',
        'Source Code': 'https://github.com/SlickQuant/slick-stream-buffer-multiplexer-py',
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Distributed Computing',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX :: Linux',
        'Operating System :: MacOS',
    ],
    keywords='stream-buffer, multiplexer, lock-free, atomic, shared-memory, ipc, multiprocessing, mpmc',
    py_modules=['slick_stream_buffer_multiplexer_py'],
    install_requires=[
        'slick-queue-py>=2.1.0',
        'slick-stream-buffer-py>=2.0.0',
    ],
    python_requires='>=3.8',
    license='MIT',
)
