.SILENT: run

export BUILD_DIR=/home/matt/jdb/

run:
	go run $(BUILD_DIR)/.

test:
	go test $(BUILD_DIR)/pkg/tests/.