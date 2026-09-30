# Maestra vector image: the upstream distroless-libc image of the same release
# with /usr/bin/vector swapped for this fork's build. The binary is compiled by
# .github/workflows/maestra.yml in rust:*-bookworm (glibc 2.36, the same as the
# base's distroless cc-debian12) and is the only file in the build context.
# Bump UPSTREAM together with the fork's rebase onto a new upstream tag.
ARG UPSTREAM=0.58.0
FROM timberio/vector:${UPSTREAM}-distroless-libc
COPY vector /usr/bin/vector
# Fails the build, not the fleet, if the binary does not load on this base.
RUN ["/usr/bin/vector", "--version"]
