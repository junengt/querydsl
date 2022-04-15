package study.querydsl.dto;

import com.querydsl.core.annotations.QueryProjection;
import lombok.Data;

@Data
public class MemberTeamDto {
    private long memberId;
    private String username;
    private int age;
    private long teamId;
    private String teamName;

    @QueryProjection
    public MemberTeamDto(long memberId, String username, int age, long teamId, String teamName) {
        this.memberId = memberId;
        this.username = username;
        this.age = age;
        this.teamId = teamId;
        this.teamName = teamName;
    }
}
