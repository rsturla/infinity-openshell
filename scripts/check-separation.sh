#!/usr/bin/env sh
set -eu

# `openshell-session` is the product-neutral core. It must not depend on the
# Infinity runtime or protocol crates. The Infinity-specific policy lives in
# `infinity-openshell`.
if cargo tree -p openshell-session -e normal --prefix none \
    | grep -Eq '^(rap-protocol|infinity-)'; then
    echo "error: openshell-session must not depend on Infinity crates" >&2
    exit 1
fi

echo "ok: openshell-session is independent of Infinity crates"
