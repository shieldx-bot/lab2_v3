import sys
import csv
from cassandra.cluster import Cluster

def export_to_csv(output_path):
    cluster = Cluster(['scylla-client.course-reg-exp.svc.cluster.local'])
    session = cluster.connect('registration')
    rows = session.execute("SELECT * FROM allocation_results")
    with open(output_path, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['student_id', 'course_id', 'section_id', 'status'])
        for row in rows:
            writer.writerow([row.student_id, row.course_id, row.section_id, row.status])

if __name__ == '__main__':
    export_to_csv(sys.argv[2])