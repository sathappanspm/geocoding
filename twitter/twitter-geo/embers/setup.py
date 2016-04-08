from distutils.core import setup, Extension
setup(name='Levenshtein', version='1.0',  \
      ext_modules=[Extension('Levenshtein', ['Levenshtein.c'])])
