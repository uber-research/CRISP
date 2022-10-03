import os
import logging
import subprocess


def aggregateCCTs(cctsAndtime):
    aggregateCcts = {}
    for time, ccts in cctsAndtime:
        for k, v in ccts.items():
            if not k in aggregateCcts:
                aggregateCcts[k] = v
            else:
                aggregateCcts[k] += v
    flameGraph = ''
    for k, v in aggregateCcts.items():
        flameGraph += k.replace('->', ';') + ' ' + str(v) + '\n'
    return flameGraph


def flameGraph(metrics, outputDir):
    # Produce SVG flame graphs from critical paths for different percentiles.
    # Returns a list of tuples [(percentile value, path to SVG file), ...]

    cctsAndtime = []
    for r in metrics:
        if not 'totalTime' in r.opTimeExclusive:
            continue
        cctsAndtime.append(
            (r.opTimeExclusive['totalTime'], r.callpathTimeExlusive))

    cctsAndtime = sorted(cctsAndtime, key=lambda x: x[0])
    percentilesExclusive = sorted([50, 95, 99])
    flameGraphPctFilePair = []
    differentialFlameGraphFiles = []
    for p in percentilesExclusive:
        limit = int(round(len(cctsAndtime) * p / 100))
        if limit == 0:
            logging.info(f"not enough samples for P" + str(p) + " flamegraph")
            continue
        flameGraph = aggregateCCTs(cctsAndtime[:limit])

        cctFile = 'flame-graph-P' + str(p) + '.cct'
        flamegraphPath = os.path.join(outputDir, cctFile)
        with open(flamegraphPath, 'w') as f:
            f.write(flameGraph)

        svgFile = flamegraphPath + '.svg'
        flameGraphPctFilePair.append(('P' + str(p), svgFile))
        with open(svgFile, 'w') as f:
            subprocess.check_call(('./flamegraph.pl', flamegraphPath),
                                  stdout=f)

        # if there are predecessors, do a differential analysis with them
        for predPct, predFile in flameGraphPctFilePair[:-1]:
            diffCCTFile = 'flame-graph-' + predPct + 'vsP' + str(p) + '.cct'
            diffFilePath = os.path.join(outputDir, diffCCTFile)
            # produce diff CCT
            with open(diffFilePath, 'w') as f:
                print(('./difffolded.pl', flamegraphPath, predFile))
                subprocess.check_call(('./difffolded.pl', '-n', predFile.rstrip('.svg'), flamegraphPath),
                                      stdout=f)
            # produce diff SVG
            diffSVGFile = diffFilePath + '.svg'
            with open(diffSVGFile, 'w') as f:
                subprocess.check_call(('./flamegraph.pl', diffFilePath),
                                      stdout=f)
            differentialFlameGraphFiles.append(diffSVGFile)

    return flameGraphPctFilePair, differentialFlameGraphFiles
