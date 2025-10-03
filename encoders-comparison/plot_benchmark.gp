#!/usr/bin/env gnuplot

# Set output format and file
set terminal png size 1200,800 enhanced font 'Verdana,10'
set output 'encoders-comparison/results/benchmark_plots.png'

# Set data format
set datafile separator ','

# Set multiplot layout
set multiplot layout 2,3 title "Proto vs C4Encoder Benchmark Results" font 'Verdana,12'

# Set common settings
set logscale x
set grid
set key left top

# Plot 1: Chat Encoding Performance
set title "Chat Data - Encoding Performance"
set xlabel "Number of Rows"
set ylabel "Time (ms)"
plot 'encoders-comparison/results/benchmark_results.csv' using 1:2 with linespoints title 'Proto Encode', \
     'encoders-comparison/results/benchmark_results.csv' using 1:3 with linespoints title 'C4Encoder Encode'

# Plot 2: Chat Decoding Performance
set title "Chat Data - Decoding Performance"
set xlabel "Number of Rows"
set ylabel "Time (ms)"
plot 'encoders-comparison/results/benchmark_results.csv' using 1:8 with linespoints title 'Proto Decode', \
     'encoders-comparison/results/benchmark_results.csv' using 1:9 with linespoints title 'C4Encoder Decode'

# Plot 3: Trade Encoding Performance
set title "Trade Data - Encoding Performance"
set xlabel "Number of Rows"
set ylabel "Time (ms)"
plot 'encoders-comparison/results/benchmark_results.csv' using 1:4 with linespoints title 'Proto Encode', \
     'encoders-comparison/results/benchmark_results.csv' using 1:5 with linespoints title 'C4Encoder Encode'

# Plot 4: Trade Decoding Performance
set title "Trade Data - Decoding Performance"
set xlabel "Number of Rows"
set ylabel "Time (ms)"
plot 'encoders-comparison/results/benchmark_results.csv' using 1:10 with linespoints title 'Proto Decode', \
     'encoders-comparison/results/benchmark_results.csv' using 1:11 with linespoints title 'C4Encoder Decode'

# Plot 5: Tweet Encoding Performance
set title "Tweet Data - Encoding Performance"
set xlabel "Number of Rows"
set ylabel "Time (ms)"
plot 'encoders-comparison/results/benchmark_results.csv' using 1:6 with linespoints title 'Proto Encode', \
     'encoders-comparison/results/benchmark_results.csv' using 1:7 with linespoints title 'C4Encoder Encode'

# Plot 6: Tweet Decoding Performance
set title "Tweet Data - Decoding Performance"
set xlabel "Number of Rows"
set ylabel "Time (ms)"
plot 'encoders-comparison/results/benchmark_results.csv' using 1:12 with linespoints title 'Proto Decode', \
     'encoders-comparison/results/benchmark_results.csv' using 1:13 with linespoints title 'C4Encoder Decode'

unset multiplot

# Create individual plots for better analysis
set output 'encoders-comparison/results/encoding_comparison.png'
set multiplot layout 1,3 title "Encoding Performance Comparison" font 'Verdana,12'

set title "Chat Encoding"
set xlabel "Number of Rows"
set ylabel "Time (ms)"
plot 'encoders-comparison/results/benchmark_results.csv' using 1:2 with linespoints title 'Proto', \
     'encoders-comparison/results/benchmark_results.csv' using 1:3 with linespoints title 'C4Encoder'

set title "Trade Encoding"
set xlabel "Number of Rows"
set ylabel "Time (ms)"
plot 'encoders-comparison/results/benchmark_results.csv' using 1:4 with linespoints title 'Proto', \
     'encoders-comparison/results/benchmark_results.csv' using 1:5 with linespoints title 'C4Encoder'

set title "Tweet Encoding"
set xlabel "Number of Rows"
set ylabel "Time (ms)"
plot 'encoders-comparison/results/benchmark_results.csv' using 1:6 with linespoints title 'Proto', \
     'encoders-comparison/results/benchmark_results.csv' using 1:7 with linespoints title 'C4Encoder'

unset multiplot

# Create decoding comparison
set output 'encoders-comparison/results/decoding_comparison.png'
set multiplot layout 1,3 title "Decoding Performance Comparison" font 'Verdana,12'

set title "Chat Decoding"
set xlabel "Number of Rows"
set ylabel "Time (ms)"
plot 'encoders-comparison/results/benchmark_results.csv' using 1:8 with linespoints title 'Proto', \
     'encoders-comparison/results/benchmark_results.csv' using 1:9 with linespoints title 'C4Encoder'

set title "Trade Decoding"
set xlabel "Number of Rows"
set ylabel "Time (ms)"
plot 'encoders-comparison/results/benchmark_results.csv' using 1:10 with linespoints title 'Proto', \
     'encoders-comparison/results/benchmark_results.csv' using 1:11 with linespoints title 'C4Encoder'

set title "Tweet Decoding"
set xlabel "Number of Rows"
set ylabel "Time (ms)"
plot 'encoders-comparison/results/benchmark_results.csv' using 1:12 with linespoints title 'Proto', \
     'encoders-comparison/results/benchmark_results.csv' using 1:13 with linespoints title 'C4Encoder'

unset multiplot


print "Benchmark plots generated successfully!"
print "Files created:"
print "- encoders-comparison/results/benchmark_plots.png"
print "- encoders-comparison/results/encoding_comparison.png"
print "- encoders-comparison/results/decoding_comparison.png"
