
curl -O "http://flvoters.com/download/20171031/11-2017%20Voter%20Extract%20Disk%20File%20Layout.docx"

curl -o history_files.html "https://flvoters.com/download/20171031/20171114_VoterHistory/"

curl -o voter_files.html "https://flvoters.com/download/20171031/20171114_VoterDetail/"

awk '/./ { f=split($0, p, "<li><a href=\""); if ( f < 2 ) { next; } split(p[2], q, "\""); print "curl -k -L -O \"https://flvoters.com/download/20171031/20171114_VoterDetail/" q[1] "\""; }' voter_files.html | grep '.txt' > voter_downloads.sh

awk '/./ { f=split($0, p, "<li><a href=\""); if ( f < 2 ) { next; } split(p[2], q, "\""); print "curl -k -L  -O \"https://flvoters.com/download/20171031/20171114_VoterHistory/" q[1] "\""; }' history_files.html | grep '.txt' > history_download.sh

