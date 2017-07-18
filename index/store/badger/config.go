package badger

import "github.com/dgraph-io/badger"

func applyIteratorOptions(o *badger.IteratorOptions, config map[string]interface{}) (*badger.IteratorOptions, error) {
	pres, ok := config["iterator_prefetch_size"].(int)
	if ok {
		o.PrefetchSize = pres
	} else {
		// defaulting to something or it will be terribly slow
		o.PrefetchSize = 100
	}

	return o, nil
}
