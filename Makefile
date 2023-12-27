build:
	docker-compose build
down:
	docker-compose down --volumes --remove-orphans
run:
	make down && docker-compose up -d
stop:
	docker-compose stop