RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

move_dir="cd share-data/db21-final-project/jars/"
reset_db="bash reset-db.sh"
kill_port="bash killbyport.sh"
server="bash server.sh"
client="bash client.sh"
exit="exit"

ssh_command="ssh -i C:\Users\user\.ssh\ming_db dbdbdb@140.113.216.170"

if [ "$1" = "1" ]
then
    echo -e "${GREEN}Node 1: DB0${NC}"
    $ssh_command -p 6666 "${move_dir}; ${reset_db}; ${kill_port}; ${server} db0 0 0; ${exit};"
elif [ "$1" = "2" ]
then
    echo -e "${GREEN}Node 2: DB1${NC}"
    $ssh_command -p 6667 "${move_dir}; ${reset_db}; ${kill_port}; ${server} db1 1 0; ${exit};"
elif [ "$1" = "3" ]
then
    echo -e "${GREEN}Node 3: Sequencer${NC}"
    $ssh_command -p 6668 "${move_dir}; ${kill_port}; ${server} dbseq 2 1; ${exit};"
elif [ "$1" = "4" ]
then
    echo -e "${GREEN}Node 4: Benchmark Client${NC}"
    $ssh_command -p 5555 "${move_dir}; ${kill_port}; ${client} 0 2; ${exit};"
elif [ "$1" = "5" ]
then
    echo -e "${GREEN}Node 4: Load Client${NC}"
    $ssh_command -p 5555 "${move_dir}; ${kill_port}; ${client} 0 1;"
fi

