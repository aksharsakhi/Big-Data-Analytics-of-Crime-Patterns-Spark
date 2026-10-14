#!/usr/bin/env bash
# ==============================================================================
# Setup Apache Spark 3.5.8 and OpenJDK 17 on macOS
# ==============================================================================
set -e

echo "=================================================================="
echo " Setting up Apache Spark 3.5.8 & OpenJDK 17 on macOS"
echo "=================================================================="

# 1. Java Environment
if [ -d "/opt/homebrew/opt/openjdk@17" ]; then
    export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
    export PATH="$JAVA_HOME/bin:$PATH"
    echo "[✓] Configured JAVA_HOME: $JAVA_HOME"
    java -version
else
    echo "[!] /opt/homebrew/opt/openjdk@17 not found yet. Please wait for Homebrew to complete."
    exit 1
fi

# 2. Extract Apache Spark
SPARK_TARBALL="$HOME/BigData/spark-3.5.8-bin-hadoop3.tgz"
SPARK_DIR="$HOME/BigData/spark-3.5.8-bin-hadoop3"

if [ -f "$SPARK_TARBALL" ]; then
    if [ ! -d "$SPARK_DIR" ]; then
        echo "[*] Extracting Spark 3.5.8 to $HOME/BigData/..."
        tar -xzf "$SPARK_TARBALL" -C "$HOME/BigData/"
        echo "[✓] Extraction complete."
    else
        echo "[✓] Spark 3.5.8 already extracted at $SPARK_DIR."
    fi
else
    echo "[!] $SPARK_TARBALL not found. Please wait for download."
    exit 1
fi

# 3. Export Environment Variables to ~/.zshrc if not present
EXPORT_SNIPPET="
# Apache Spark & Java Configuration (Big Data Analytics)
export JAVA_HOME=\"/opt/homebrew/opt/openjdk@17\"
export SPARK_HOME=\"\$HOME/BigData/spark-3.5.8-bin-hadoop3\"
export PATH=\"\$SPARK_HOME/bin:\$JAVA_HOME/bin:\$PATH\"
"

if ! grep -q "SPARK_HOME.*spark-3.5.8-bin-hadoop3" "$HOME/.zshrc" 2>/dev/null; then
    echo "$EXPORT_SNIPPET" >> "$HOME/.zshrc"
    echo "[✓] Persisted SPARK_HOME and JAVA_HOME to ~/.zshrc."
fi

export SPARK_HOME="$SPARK_DIR"
export PATH="$SPARK_HOME/bin:$PATH"

echo "=================================================================="
echo " [SUCCESS] Apache Spark is ready to run on macOS!"
echo " Test with:"
echo "   spark-shell --version"
echo "=================================================================="
