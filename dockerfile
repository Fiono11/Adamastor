# Use Rust nightly image to build Rust code
FROM rustlang/rust:nightly as build

WORKDIR /usr/src/dringct

# Install dependencies
RUN apt-get update \
    && apt-get install -y clang libclang-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy Rust code and build it
COPY . ./
RUN cargo build --release

# Start a new stage from Python image
FROM python:3.9-slim-buster

WORKDIR /usr/src/benchmark

# Copy Python requirements from benchmark directory and install them
COPY ./benchmark/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy Rust binary from previous stage
COPY --from=build /usr/src/dringct/target/release/node ../dringct

# Copy the rest of your Python application from benchmark directory
COPY ./benchmark ./

CMD ["fab local"]
