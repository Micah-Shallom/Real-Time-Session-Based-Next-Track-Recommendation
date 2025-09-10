FROM bitnami/spark:3.5.0

# Switch to root user to install packages
USER root

# Create pip config directory and set timeout/retry configurations
RUN mkdir -p /root/.config/pip && \
    echo "[global]" > /root/.config/pip/pip.conf && \
    echo "timeout = 300" >> /root/.config/pip/pip.conf && \
    echo "retries = 5" >> /root/.config/pip/pip.conf && \
    echo "trusted-host = pypi.org" >> /root/.config/pip/pip.conf && \
    echo "               files.pythonhosted.org" >> /root/.config/pip/pip.conf

# Install Python dependencies with extended timeout and retries
RUN pip install --no-cache-dir --timeout=300 --retries=5 \
    kafka-python==2.0.2 \
    pymongo==4.5.0 \
    pandas==2.0.3 \
    numpy==1.24.3

# Switch back to non-root user for security
# USER 1001

# Set working directory
WORKDIR /app