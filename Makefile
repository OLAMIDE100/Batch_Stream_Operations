batch_start:
		docker-compose -f Batch_operations/docker/docker-compose.yml up -d




batch_stop:
		docker-compose -f Batch_operations/docker/docker-compose.yml down




stream_start:
		docker-compose -f Stream_operations/docker-compose.yml up -d

stream_stop:
		docker-compose -f Stream_operations/docker-compose.yml down
