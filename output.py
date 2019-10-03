import csv
from benchmark.query import TaggedValidationResult
from benchmark.query import TaggedValidationResultUptime, Category
from benchmark.utils import RuntimeObjects
import collections


def convert_to_csv(validation_results: [TaggedValidationResult], uptime_results: [TaggedValidationResultUptime]):
    validation_lines = collections.defaultdict(lambda: [])
    validation_lines[Category.GRID] = []
    validation_lines[Category.SYMPHONY] = []
    uptime_lines = []
    benchmark_result = './tmp/benchmark_result'
    uptimeinfo = './tmp/uptime_result.csv'
    infofile = './tmp/infofile'

    for result in validation_results:
        metric = result.metric
        for tag, change_result in result.tag_to_change_results.items():
            line = [
                metric.name + '.' + (tag or ''),
                change_result.baseline_value,
                change_result.current_value,
                'FAILED' if change_result.is_failure else 'PASSED'
            ]
            validation_lines[metric.category].append(line)

    for result in uptime_results:
        metric = result.metric

        for tag, change_result in result.tag_to_change_results.items():
            line = [
                metric.name + '.' + (tag or ''),
                change_result.current_value,
                'FAILED' if change_result.is_failure else 'PASSED'
            ]
            uptime_lines.append(line)

    with open(benchmark_result + '.csv', 'w') as writeFile:
        writer = csv.writer(writeFile, delimiter=':')
        writer.writerows(validation_lines[Category.INDEXER]+ validation_lines[Category.UNKNOWN])
    writeFile.close()
    with open(benchmark_result + '_g.csv', 'w') as writeFile:
        writer = csv.writer(writeFile, delimiter=':')
        writer.writerows(validation_lines[Category.GRID])
    writeFile.close()
    with open(benchmark_result + '_s.csv', 'w') as writeFile:
        writer = csv.writer(writeFile, delimiter=':')
        writer.writerows(validation_lines[Category.SYMPHONY])
    writeFile.close()
    with open(uptimeinfo, 'w') as writeFile:
        writer = csv.writer(writeFile, delimiter=':')
        writer.writerows(uptime_lines)
    writeFile.close()
    with open(infofile, 'w') as writeFile:
        writeFile.write(RuntimeObjects.info)
    writeFile.close()
