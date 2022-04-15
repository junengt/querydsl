package study.querydsl.dto;

import lombok.Data;

@Data
public class MemberSearchCondition {
    //회원명, 팀명, 나이(ageGoe, ageLoe)

    private String username;
    private String teamName;
    private Integer ageGoe; //int가 아니라 Integer인 이유는 나이가 null일 수 있기 때문
    private Integer ageLoe;
}
