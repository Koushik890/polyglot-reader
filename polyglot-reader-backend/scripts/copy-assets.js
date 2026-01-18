/* eslint-disable no-console */
const fs = require('fs');
const path = require('path');

const srcDir = path.join(__dirname, '..', 'src', 'assets');
const distDir = path.join(__dirname, '..', 'dist', 'assets');

function copyDirRecursive(from, to) {
  if (!fs.existsSync(from)) return;
  fs.mkdirSync(to, { recursive: true });
  const entries = fs.readdirSync(from, { withFileTypes: true });
  for (const entry of entries) {
    const srcPath = path.join(from, entry.name);
    const dstPath = path.join(to, entry.name);
    if (entry.isDirectory()) {
      copyDirRecursive(srcPath, dstPath);
    } else if (entry.isFile()) {
      fs.copyFileSync(srcPath, dstPath);
    }
  }
}

try {
  copyDirRecursive(srcDir, distDir);
  console.log(`[copy-assets] Copied assets to ${distDir}`);
} catch (err) {
  console.error('[copy-assets] Failed to copy assets:', err);
  process.exitCode = 1;
}

