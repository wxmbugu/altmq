test:test_dir
	cargo test -- --nocapture

test_dir:
	mkdir -p  test_data/segments 

clean:
	rm -r test_data

