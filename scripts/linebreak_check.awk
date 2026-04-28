BEGIN {
    FS = "|"        # 구분자
}
FNR == 1 {
    EXPC = NF - 1
}
{
    # 1천만 줄마다 진행 표시
    if (FNR % 10000000 == 0) {
        printf "." > "/dev/stderr"
        fflush("/dev/stderr")
    }

    if (EXPC != NF - 1) {
        print "  Line " FNR " has more or less " EXPC " pipes" > "/dev/stderr"
        print $0 > "/dev/stderr"
        exit 1
    }
}
