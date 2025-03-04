// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/dustin/go-humanize"

	"github.com/f5/otel-arrow-adapter/pkg/benchmark"
	"github.com/f5/otel-arrow-adapter/pkg/benchmark/dataset"
	"github.com/f5/otel-arrow-adapter/pkg/benchmark/profileable/arrow"
	"github.com/f5/otel-arrow-adapter/pkg/benchmark/profileable/otlp"
)

var help = flag.Bool("help", false, "Show help")

func main() {
	// Parse the flag
	flag.Parse()

	// Usage Demo
	if *help {
		flag.Usage()
		os.Exit(0)
	}

	// Define default input file
	inputFiles := flag.Args()
	if len(inputFiles) == 0 {
		inputFiles = append(inputFiles, "./data/otlp_metrics.pb")
	}

	warmUpIter := uint64(2)

	// Performance comparison for each input file
	for i := range inputFiles {
		// Compare the performance between the standard OTLP representation and the OTLP Arrow representation.
		profiler := benchmark.NewProfiler([]int{10, 100, 1000, 2000, 4000}, "output/metrics_benchmark.log", warmUpIter)
		compressionAlgo := benchmark.Zstd()
		maxIter := uint64(3)
		ds := dataset.NewRealMetricsDataset(inputFiles[i])
		profiler.Printf("Dataset '%s' (%s) loaded\n", inputFiles[i], humanize.Bytes(uint64(ds.SizeInBytes())))
		otlpMetrics := otlp.NewMetricsProfileable(ds, compressionAlgo)
		otlpArrowMetrics := arrow.NewMetricsProfileable([]string{}, ds, &benchmark.Config{})

		if err := profiler.Profile(otlpMetrics, maxIter); err != nil {
			panic(fmt.Errorf("expected no error, got %v", err))
		}
		if err := profiler.Profile(otlpArrowMetrics, maxIter); err != nil {
			panic(fmt.Errorf("expected no error, got %v", err))
		}

		profiler.CheckProcessingResults()

		// Configure the profile output
		benchmark.OtlpArrowConversionSection.CustomColumnFor(otlpMetrics).
			MetricNotApplicable()

		profiler.Printf("\nMetrics dataset summary:\n")
		profiler.Printf("- #metrics: %d\n", ds.Len())

		profiler.PrintResults(maxIter)

		profiler.ExportMetricsTimesCSV(fmt.Sprintf("%d_metrics_benchmark_results", i))
		profiler.ExportMetricsBytesCSV(fmt.Sprintf("%d_metrics_benchmark_results", i))
	}
}
