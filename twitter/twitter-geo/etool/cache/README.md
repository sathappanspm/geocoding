The scripts in this directory all relate to common cache functions that are cache system agnostic.

# Python scripts
* cache.py - Discovers all queues for the 'hostname' provided that have the 'cache: true' option set in 
            embers.conf and caches their messages. Also used for the EMBERS cache service.