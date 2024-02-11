# syntax=docker/dockerfile:1

# Use the official Golang image to create a build artifact
FROM golang:1.21.5

# Set destination for COPY
WORKDIR /app

# Download Go modules 
COPY go.mod   ./
RUN go mod download

# Copy all contents of root directory (copy all to include subdirectories like btree containing source code)
COPY . ./

# Build with static linking for portability (?) and linux OS
RUN CGO_ENABLED=0 GOOS=linux go build -o /docker-go-db

# Run
CMD ["/docker-go-db"]

# To build the image, run the following command in the directory where the Dockerfile is located:
# docker build --tag docker-go-db .

# To run the image, execute the following command:
# docker run docker-go-db                                   

# Tutorial: https://docs.docker.com/language/golang/build-images/