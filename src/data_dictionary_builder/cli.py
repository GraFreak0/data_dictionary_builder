# Generate secure keys
data-dictionary generate-keys

# Install web UI
data-dictionary install-web-ui --generate-keys

# Start/stop web UI
data-dictionary start-web-ui
data-dictionary stop-web-ui

# Extract metadata
data-dictionary extract --db-type postgres --host localhost --user readonly

# Show info
data-dictionary info
data-dictionary --version