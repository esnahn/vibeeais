BEGIN {
    if (ARGC < 3) {
        print "usage: awk -f script.awk input output" > "/dev/stderr"
        exit 1
    }
    out = ARGV[2]
    delete ARGV[2]  # prevent awk from treating it as input file

    FS = "|"        # 구분자
    buf = ""        # 레코드 조립용 버퍼
    sep_count = 0   # buf에 누적된 구분자 개수
}
FNR == 1 {
    EXPC = NF - 1
}
{
    # 20만 줄마다 진행 표시
    if (FNR % 200000 == 0) {
        printf "." > "/dev/stderr"
        # 1천만 줄마다 줄바꿈
        if (FNR % 10000000 == 0) {
            printf "\n" > "/dev/stderr"
        }
        fflush("/dev/stderr")
    }

    line = $0

    # 구분자(FS)로 분리된 (컬럼 수 - 1)로 구분자 수 계산
    this_count = NF - 1

    if (buf == "") {
        # 버퍼 없으면 새 레코드 시작
        buf = line
        sep_count = this_count
    } else {
        # 아직 구분자가 부족했으면 다음 줄 이어붙이기
        # 공백으로 대체하여 연결
        buf = buf " " line
        sep_count += this_count
    }

    if (sep_count == EXPC) {
        # 기대치(EXPC)와 일치하면, 완성된 레코드를 out 파일로 출력
        print buf > out

        # 버퍼 초기화
        buf = ""
        sep_count = 0
    }
    else if (sep_count > EXPC) {
        # 구분자 초과 시 에러 출력
        print "WARNING: pipe overflow in record:\n" buf > "/dev/stderr"

        # 버퍼 초기화
        buf = ""
        sep_count = 0
    }
    # sep_count < EXPC 이면, 아직 레코드가 미완성이므로 대기
}
END {
    # 남은 버퍼가 있으면 마지막 레코드로 출력
    if (buf != "") print buf > out

    # 마지막 줄이 진행 표시 문자열로 끝나면 줄바꿈 (200000개 미만일 경우 새 줄에는 아직 점이 찍히지 않음)
    if (FNR % 10000000 >= 200000) {
        printf "\n" > "/dev/stderr"
    }
}
