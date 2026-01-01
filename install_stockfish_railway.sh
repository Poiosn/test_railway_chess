#!/bin/bash
# Install Stockfish on Railway deployment

echo "📥 Installing Stockfish..."

# Try apt-get (Debian/Ubuntu)
if command -v apt-get &> /dev/null; then
    apt-get update
    apt-get install -y stockfish
    echo "✅ Stockfish installed via apt-get"
    exit 0
fi

# Try downloading binary directly
echo "Downloading Stockfish binary..."
curl -L "https://github.com/official-stockfish/Stockfish/releases/download/sf_17/stockfish-ubuntu-x86-64-avx2.tar" -o stockfish.tar
tar -xf stockfish.tar
mv stockfish/stockfish-ubuntu-x86-64-avx2 ./stockfish
chmod +x ./stockfish
rm -rf stockfish.tar
echo "✅ Stockfish installed from GitHub"
