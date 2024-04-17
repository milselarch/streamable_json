from JsonStreamWriter import JsonStreamWriter

filename = 'test.json'

with JsonStreamWriter(filename) as writer:
    raise NotImplementedError