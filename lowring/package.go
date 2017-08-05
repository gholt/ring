// Package lowring is the low-level code underpinning github.com/gholt/ring.
//
// It is assumed if you are working with this package that you are very
// familiar with github.com/gholt/ring and don't need the extra wrappings that
// provides or want to provide your own wrappings. Items that are documented in
// github.com/gholt/ring will not be documented here, or at least not as fully.
// Some of the examples are still in-depth, as they provide both testing and
// explanation.
//
// This package tries to be as simple and concise as possible, which is still
// pretty complex considering the subject matter. For example, nodes and tiers
// are just represented by indexes -- no names, IPs, etc. -- it is left to the
// user of the package to maintain such metadata.
//
// This package also aims for speed, which fights against the previous goal.
// Building and maintaining rings can be resource intensive, and optimizations
// can greatly reduce this impact. But optimizations often add complexity and
// reduce flexibility.
package lowring
