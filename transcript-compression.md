# Transcript database compression study

Date: 2026-09-08

This study measures a representative transcript database copied from a test
device. The device-specific source path has been omitted.

This is a research artifact, not the current configuration contract. The
production `StoragePolicy` accepts 8--112 KiB dictionaries; larger candidates
below describe possible future work and are not currently selectable.

The local source was 82.75 MiB with 84,740 1 KiB SQLite pages. Its copied WAL
was empty. SQLite `VACUUM INTO` produced logically equivalent 4 KiB and 8 KiB
versions; both passed `PRAGMA integrity_check`.

Unless noted otherwise, the tests used:

- Zstandard 1.5.7 at level 3;
- independent SQLite-page frames;
- a raw fallback when compression saved fewer than 64 bytes;
- dictionaries trained by `ZDICT_trainFromBuffer`, matching
  `zstd::dict::from_samples`;
- binary MiB and KiB units; and
- the V6 segment estimate used by `benches/page_dictionary.rs`.

The sidecar estimate includes stored page payloads, an 80-byte frame header,
a 60-byte index entry per stored page, the dictionary table, the conservative
full-page-map upper bound, and segment header/trailer bytes.

## Page-size comparison

The stock benchmark trains a 64 KiB dictionary from 8,192 evenly distributed
pages. Consequently its training input varies with SQLite page size.

| SQLite page | SQLite size | Training input | 64 KiB windows | Page Zstd, no dictionary | Page Zstd + 64 KiB dictionary |
|---:|---:|---:|---:|---:|---:|
| 1 KiB | 82.75 MiB | 8 MiB | 26.41 MiB (31.917%) | 58.06 MiB (70.158%) | 43.58 MiB (52.659%) |
| 4 KiB | 81.88 MiB | 32 MiB | 21.99 MiB (26.862%) | 35.77 MiB (43.683%) | 26.12 MiB (31.897%) |
| 8 KiB | 82.98 MiB | 64 MiB | 21.69 MiB (26.142%) | 31.12 MiB (37.504%) | 23.03 MiB (27.757%) |

The original 1 KiB layout is hostile to per-page framing. Its fixed frame and
index records alone consume 11.31 MiB. Repacking to 4 KiB reduces that figure
to 2.80 MiB. The rest of this study therefore uses the requested 4 KiB layout.

A second page-size run held training input fixed at 32 MiB:

| SQLite page | Dictionary payload | Estimated sidecar | Sidecar/database |
|---:|---:|---:|---:|
| 1 KiB | 30.473 MiB | 43.474 MiB | 52.534% |
| 4 KiB | 22.847 MiB | 26.115 MiB | 31.897% |
| 8 KiB | 21.210 MiB | 22.902 MiB | 27.600% |

## Training-input sweep for a 64 KiB dictionary

Every candidate was evaluated against the complete 81.875 MiB, 4 KiB-page
database. Samples were pages selected evenly over the database.

| Training input | Samples | Training time | Payload | Estimated sidecar | Sidecar/database |
|---:|---:|---:|---:|---:|---:|
| 1 MiB | 256 | 0.033 s | 23.872 MiB | 27.141 MiB | 33.149% |
| 2 MiB | 512 | 0.069 s | 23.329 MiB | 26.598 MiB | 32.486% |
| 4 MiB | 1,024 | 0.148 s | 22.966 MiB | 26.234 MiB | 32.042% |
| **8 MiB** | **2,048** | **0.298 s** | **22.753 MiB** | **26.022 MiB** | **31.783%** |
| 16 MiB | 4,096 | 0.970 s | 22.765 MiB | 26.034 MiB | 31.797% |
| 32 MiB | 8,192 | 1.219 s | 22.847 MiB | 26.116 MiB | 31.897% |
| 48 MiB | 12,288 | 2.379 s | 22.857 MiB | 26.125 MiB | 31.909% |
| 64 MiB | 16,384 | 3.009 s | 22.845 MiB | 26.114 MiB | 31.895% |
| 80 MiB | 20,480 | 4.531 s | 22.845 MiB | 26.114 MiB | 31.895% |

For a 64 KiB dictionary, 8 MiB is the measured knee and the numerical winner.
More training bytes did not improve this database.

## Dictionary-size sweep with the full database as training input

This sweep gives each dictionary the same maximum 81.875 MiB corpus and
charges the actual returned dictionary bytes to the segment.

| Requested dictionary | Actual dictionary | Training time | Payload | Estimated sidecar | Sidecar/database |
|---:|---:|---:|---:|---:|---:|
| 64 KiB | 64 KiB | 5.486 s | 22.841 MiB | 26.110 MiB | 31.890% |
| 128 KiB | 128 KiB | 10.616 s | 20.929 MiB | 24.260 MiB | 29.630% |
| 256 KiB | 256 KiB | 4.609 s | 19.528 MiB | 22.984 MiB | 28.072% |
| 384 KiB | 384 KiB | 3.590 s | 19.128 MiB | 22.709 MiB | 27.736% |
| 512 KiB | 512 KiB | 3.449 s | 18.650 MiB | 22.356 MiB | 27.305% |
| 640 KiB | 640 KiB | 6.188 s | 18.332 MiB | 22.163 MiB | 27.070% |
| 768 KiB | 768 KiB | 4.632 s | 18.230 MiB | 22.186 MiB | 27.097% |
| **896 KiB** | **896 KiB** | **5.023 s** | **18.004 MiB** | **22.085 MiB** | **26.974%** |
| 1 MiB | 1 MiB | 5.520 s | 17.933 MiB | 22.139 MiB | 27.040% |
| 1.25 MiB | 1.25 MiB | 4.742 s | 18.349 MiB | 22.805 MiB | 27.854% |
| 1.5 MiB | 1.5 MiB | 3.963 s | 19.139 MiB | 23.846 MiB | 29.124% |

Capacities of 2, 3, 4, 6, and 8 MiB did not produce correspondingly large
dictionaries with this trainer. It returned only about 2.3--3.2 KiB, and the
sidecars regressed to roughly 35.4 MiB (43.3%). They are not viable results.

The useful region is a broad 640 KiB--1 MiB plateau. The 896 KiB candidate is
the numerical full-corpus winner, but it beats 640 KiB by only 80 KiB over the
entire database. A 1 MiB dictionary has slightly smaller payload than 896 KiB,
but its additional dictionary bytes make the complete segment 55 KiB larger.
Requested sizes beyond 1 MiB do not help with the current trainer.

The 896 KiB result is almost identical to the 21.99 MiB estimate for 64 KiB
compression windows while retaining 4 KiB point-read granularity.

## Training-input sweep for an 896 KiB dictionary

The default trainer is sharply non-monotonic at this size. Every result below
was independently trained and evaluated over the complete database.

| Training input | Training time | Payload | Estimated sidecar | Sidecar/database |
|---:|---:|---:|---:|---:|
| 8 MiB | 0.423 s | 23.438 MiB | 26.995 MiB | 32.971% |
| 12 MiB | 0.576 s | 23.136 MiB | 26.912 MiB | 32.870% |
| 16 MiB | 0.746 s | 23.481 MiB | 27.188 MiB | 33.207% |
| 20 MiB | 1.050 s | 22.842 MiB | 26.732 MiB | 32.650% |
| 24 MiB | 1.992 s | 18.083 MiB | 22.164 MiB | 27.070% |
| 28 MiB | 2.175 s | 22.915 MiB | 26.827 MiB | 32.766% |
| **32 MiB** | **1.329 s** | **17.992 MiB** | **22.074 MiB** | **26.960%** |
| 40 MiB | 1.753 s | 18.095 MiB | 22.176 MiB | 27.086% |
| 48 MiB | 2.480 s | 18.068 MiB | 22.149 MiB | 27.053% |
| **56 MiB** | **3.669 s** | **17.953 MiB** | **22.035 MiB** | **26.912%** |
| 64 MiB | 2.506 s | 18.028 MiB | 22.109 MiB | 27.003% |
| 72 MiB | 2.505 s | 18.001 MiB | 22.083 MiB | 26.971% |
| 80 MiB | 2.504 s | 18.068 MiB | 22.150 MiB | 27.053% |
| All 81.875 MiB | 5.023 s | 18.004 MiB | 22.085 MiB | 26.974% |

The numerical winner is 56 MiB, but it saves only about 40 KiB relative to the
32 MiB candidate and takes substantially longer to train. More input is not
monotonically better: the 28 MiB candidate is dramatically worse than both 24
and 32 MiB. A production implementation must score the generated candidate
against current-dictionary and no-dictionary baselines; it cannot infer quality
from sample byte count.

For this database, 32 MiB is therefore a reasonable cost/quality point for an
896 KiB dictionary even though it was not imposed as a limit. If seal work is
fully asynchronous, 56 MiB is the measured size winner, but only marginally.

## Cost of embedding dictionaries in small segments

Dictionary bytes alone consume the following fraction of raw segment payload.
This table assumes the dictionary is embedded independently in every segment.

| Dictionary | 1 MiB segment | 4 MiB | 8 MiB | 16 MiB | 32 MiB | 64 MiB |
|---:|---:|---:|---:|---:|---:|---:|
| 64 KiB | 6.25% | 1.56% | 0.78% | 0.39% | 0.20% | 0.10% |
| 256 KiB | 25.00% | 6.25% | 3.12% | 1.56% | 0.78% | 0.39% |
| 512 KiB | 50.00% | 12.50% | 6.25% | 3.12% | 1.56% | 0.78% |
| 640 KiB | 62.50% | 15.62% | 7.81% | 3.91% | 1.95% | 0.98% |
| 896 KiB | 87.50% | 21.88% | 10.94% | 5.47% | 2.73% | 1.37% |
| 1 MiB | 100.00% | 25.00% | 12.50% | 6.25% | 3.12% | 1.56% |

Using the measured full-database payload ratios, and assuming a smaller segment
has the same content mix and can reuse a dictionary of the same quality, the
modeled size winner changes with segment size:

| Raw segment payload | Modeled embedded-dictionary winner |
|---:|---:|
| 1--2 MiB | 64 KiB |
| 3--7 MiB | 128 KiB |
| 8--23 MiB | 256 KiB |
| 24--32 MiB | 512 KiB |
| 33--62 MiB | 640 KiB |
| 63--144 MiB | 896 KiB |
| 145 MiB and larger | 1 MiB |

This is an amortization model, not a direct benchmark of newly trained tiny
segments. Its breakpoints exclude overhead common to every dictionary choice.
The most important example is the 64-to-896 KiB transition: the larger
dictionary saves about 5.9 percentage points of page payload but adds 832 KiB,
so it breaks even at roughly 14 MiB of raw pages.

Small current-format segments also repeat costs unrelated to dictionary size:

- frame header plus index entry: 140 bytes per 4 KiB stored page, or 3.42%;
- segment header and trailer: approximately 8 KiB; and
- the benchmark's conservative full map for this database: approximately
  0.40 MiB per segment.

That repeated full map alone is about 40% of a 1 MiB delta, 10% of a 4 MiB
delta, 5% of an 8 MiB delta, and 1.25% of a 32 MiB delta. Small live segments
therefore need attention to both dictionary placement and map representation.

## Recommendation from this database

There should not be one embedded dictionary size for every segment.

- For an approximately 82 MiB snapshot, use an 896 KiB candidate and score it
  before publication. A 640 KiB candidate is nearly as good and cheaper to
  cache.
- For the expected 24--32 MiB live-segment range, start with a 512 KiB
  dictionary; an embedded 896 KiB dictionary is not yet fully amortized.
- For segments below 8 MiB, use 64--128 KiB or retain them raw until enough
  material accumulates.
- Train from all available useful pages when convenient, but test the result.
  For the 896 KiB candidate, 32 MiB was within 40 KiB of the best measured
  56 MiB result.
- Keep dictionary size configurable and select from a small ladder such as
  64, 128, 256, 512, 640, and 896 KiB based on expected segment payload.
- Do not extend the current trainer past 1 MiB based on these results.

The compression-time measurements above are useful only directionally: the
benchmark was single-process and hot-cache timing varied between passes. The
byte counts are deterministic and are the basis for the recommendations.
