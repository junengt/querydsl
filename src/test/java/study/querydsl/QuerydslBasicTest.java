package study.querydsl;

import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.Projections;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.assertj.core.api.Assertions;
import org.h2.engine.User;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.dto.MemberDto;
import study.querydsl.dto.UserDto;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.QTeam;
import study.querydsl.entity.Team;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceUnit;
import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static study.querydsl.entity.QMember.*;
import static study.querydsl.entity.QMember.member;
import static study.querydsl.entity.QTeam.*;

@SpringBootTest
@Transactional
public class QuerydslBasicTest {

    @Autowired
    EntityManager em;

    JPAQueryFactory query; // 필드로 빼게 되면 동시성 문제가 발생하지 않는다 이유는 멀티 쓰레드 환경에서 문제 없이 작동 가능

    @BeforeEach//각각 개별 Test 실행 전에 미리 실행됨
    public void before() {
        query = new JPAQueryFactory(em);
        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);

        Member member1 = new Member("member1",10, teamA);
        Member member2 = new Member("member2",20, teamA);

        Member member3 = new Member("member3",30, teamB);
        Member member4 = new Member("member4",40, teamB);
        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);
    }

    @Test
    public void startJPQL() {
        //member1 찾기
        Member findMember = em.createQuery("select m from Member m where m.userName = :userName", Member.class)
                .setParameter("userName", "member1")
                .getSingleResult();

        assertThat(findMember.getUserName()).isEqualTo("member1");
    }

    @Test
    public void startQuerydsl() {
        //QMember m = new QMember("m");처럼 별칭을 지정해서 사용하는 경우는 같은 테이블을 조인해서 사용할 때
        //그 외의 경우에는 static import로 선언해서 사용한다
        Member findMember = query
                .select(member)
                .from(member)
                .where(member.userName.eq("member1")) //파라미터 바인딩 처리
                .fetchOne();

        assertThat(findMember.getUserName()).isEqualTo("member1");
    }

    @Test
    public void search() {
        Member findMember = query
                .selectFrom(member)
                .where(member.userName.
                        isNotNull()
                        .and(member.age.eq(10)))
                .fetchOne();
        assertThat(findMember.getUserName()).isEqualTo("member1");
    }

    @Test
    public void searchAndParam() {
        Member findMember = query
                .selectFrom(QMember.member)
                .where(QMember.member.userName.eq("member1"),
                        QMember.member.age.eq(10)
                )
                .fetchOne();
        assertThat(findMember.getUserName()).isEqualTo("member1");
    }

    @Test
    public void resultFetch() {
        //List 조회
//        List<Member> fetch = query
//                .selectFrom(member)
//                .fetch();
//
        //단건 조회
//        Member fetchOne = query
//                .selectFrom(member)
//                .fetchOne();
//
//        Member fetchFirst = query
//                .selectFrom(QMember.member)
//                .fetchFirst();//.limit(1).fetchOne();이랑 똑같음

//        QueryResults<Member> result = query
//                .selectFrom(member)
//                .fetchResults();
//
//        result.getTotal();
//        List<Member> content = result.getResults();

        long count = query
                .selectFrom(member)
                .fetchCount();

    }

    /**
     * 회원 정렬 순서
     * 1. 회원 나이 내림차순(desc)
     * 2. 회원 이름 올림차순(asc)
     * 단 2에서 회원 이름이 없으면 마지막에 출력(nulls last)
     */
    @Test
    public void sort() {
        em.persist(new Member(null, 100));
        em.persist(new Member("member5",100));
        em.persist(new Member("member6", 100));

        List<Member> result = query
                .selectFrom(member)
                .where(member.age.eq(100))
                .orderBy(member.age.desc(), member.userName.asc().nullsLast())
                .fetch();

        Member member5 = result.get(0);
        Member member6 = result.get(1);
        Member memberNull = result.get(2);
        assertThat(member5.getUserName()).isEqualTo("member5");
        assertThat(member6.getUserName()).isEqualTo("member6");
        assertThat(memberNull.getUserName()).isNull();
    }

    @Test
    public void paging1() {
        List<Member> result = query
                .selectFrom(member)
                .orderBy(member.userName.asc())
                .offset(1)
                .limit(2)
                .fetch();

        assertThat(result.size()).isEqualTo(2);
    }

    @Test
    public void paging2() {
        QueryResults<Member> result = query
                .selectFrom(member)
                .orderBy(member.userName.asc())
                .offset(1)
                .limit(2)
                .fetchResults();

        assertThat(result.getTotal()).isEqualTo(4);
        assertThat(result.getLimit()).isEqualTo(2);
        assertThat(result.getOffset()).isEqualTo(1);
        assertThat(result.getResults().size()).isEqualTo(2);
    }

    @Test
    public void aggregation() {
        //querydsl이 제공하는 Tuple
        List<Tuple> result = query
                .select(member.count(),
                        member.age.sum(),
                        member.age.avg(),
                        member.age.max(),
                        member.age.min()
                )
                .from(member)
                .fetch();

        Tuple tuple = result.get(0);
        assertThat(tuple.get(member.count())).isEqualTo(4);
        assertThat(tuple.get(member.age.sum())).isEqualTo(100);
        assertThat(tuple.get(member.age.avg())).isEqualTo(25);
        assertThat(tuple.get(member.age.max())).isEqualTo(40);
        assertThat(tuple.get(member.age.min())).isEqualTo(10);
    }

    /**
     * 팀의 이름과 각 팀의 평균 연령을 구해라.
     * @throws Exception
     */
    @Test
    public void group() throws Exception {
        List<Tuple> result = query
                .select(team.name, member.age.avg())
                .from(member)
                .join(member.team, team)
                .groupBy(team.name)
                .fetch();

        Tuple teamA = result.get(0);
        Tuple teamB = result.get(1);

        assertThat(teamA.get(team.name)).isEqualTo("teamA");
        assertThat(teamA.get(member.age.avg())).isEqualTo(15); //(10+20) / 2
        assertThat(teamB.get(team.name)).isEqualTo("teamB");
        assertThat(teamB.get(member.age.avg())).isEqualTo(35); //(30+40) / 2
    }

    /**
     * 팀 A에 소속된 모든 회원을 찾아라
     */
    @Test
    public void join() {
        List<Member> result = query
                .selectFrom(member)
                .join(member.team, team)
                .where(team.name.eq("teamA"))
                .fetch();

        assertThat(result)
                .extracting("userName")
                .containsExactly("member1","member2");
    }

    /**
     * 세타 조인
     * 회원의 이름이 팀의 이름과 같은 회원 조회
     */
    //연관 관계가 없어도 조인할 수 있는 방법
    @Test
    public void theta_join() {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));

        List<Member> result = query
                .selectFrom(member)
                .from(member, team)
                .where(member.userName.eq(team.name))
                .fetch();

        assertThat(result)
                .extracting("userName")
                .containsExactly("teamA","teamB");
//        assertThat(result.get(0).getUserName()).isEqualTo("teamA");
//        assertThat(result.get(1).getUserName()).isEqualTo("teamB");
    }

    /**
     * 회원과 팀을 조인하면서, 팀 이름이 teamA인 팀만 조인, 회원은 모두 조회
     */
    @Test
    public void join_on_filtering() {
        List<Tuple> result = query
                .select(member, team)
                .from(member)
                .leftJoin(member.team, team)
                .on(team.name.eq("teamA"))
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    @Test
    public void join_on_no_relation() {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));

        List<Tuple> result = query
                .select(member, team)
                .from(member)
                .leftJoin(team).on(member.userName.eq(team.name))
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    @PersistenceUnit
    EntityManagerFactory emf;

    @Test
    public void fetchJoinNo() {
        em.flush();
        em.clear();

        Member findMember = query
                .selectFrom(QMember.member)
                .where(QMember.member.userName.eq("member1"))
                .fetchOne();

        //이미 로딩된 엔티티인지 로딩이 안된 엔티티인지 알려주는 함수(isLoaded)
        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).as("페치 조인 미적용").isFalse();
    }

    //난 멤버 조회할 때 연관된 팀도 같이 조회할 거야
    @Test
    public void fetchJoinUse() {
        em.flush();
        em.clear();

        Member findMember = query
                .selectFrom(member)
                .join(member.team, team).fetchJoin()
                .where(member.userName.eq("member1"))
                .fetchOne();

        //이미 로딩된 엔티티인지 로딩이 안된 엔티티인지 알려주는 함수(isLoaded)
        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).as("페치 조인 적용").isTrue();
    }

    /**
     * 나이가 가장 많은 회원 조회
     */
    @Test
    public void subQuery() {

        QMember memberSub = new QMember("memberSub");
        List<Member> result = query
                .selectFrom(member)
                .where(member.age.eq(
                        JPAExpressions
                                .select(memberSub.age.max())
                                .from(memberSub)
                ))
                .fetch();

        assertThat(result).extracting("age")
                .containsExactly(40);
    }

    /**
     * 나이가 평균 이상인 회원 조회
     */
    @Test
    public void subQueryGoe() {

        QMember memberSub = new QMember("memberSub");
        List<Member> result = query
                .selectFrom(member)
                .where(member.age.goe(
                        JPAExpressions.select(memberSub.age.avg())
                                .from(memberSub)
                ))
                .fetch();

        assertThat(result).extracting("age")
                .containsExactly(30,40);
    }

    /**
     * 나이가 평균 이상인 회원 조회
     */
    @Test
    public void subQueryIn() {

        QMember memberSub = new QMember("memberSub");

        List<Member> result = query
                .selectFrom(member)
                .where(member.age.in(
                        JPAExpressions.select(memberSub.age)
                                .from(memberSub)
                                .where(memberSub.age.gt(10))
                ))
                .fetch();

        assertThat(result).extracting("age")
                .containsExactly(20, 30,40);
    }

    @Test
    public void selectSubQuery() {

        QMember memberSub = new QMember("memberSub");

        List<Tuple> result = query
                .select(member.userName,
                        JPAExpressions
                                .select(memberSub.age.avg())
                                .from(memberSub))
                .from(member)
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    @Test
    public void basicCase() {
        List<String> result = query
                .select(member.age
                        .when(10).then("열살")
                        .when(20).then("스무살")
                        .otherwise("기타"))
                .from(member)
                .fetch();
        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    @Test
    public void complexCase() {
        List<String> result = query
                .select(new CaseBuilder()
                        .when(member.age.between(0, 20)).then("0살~20살")
                        .when(member.age.between(21, 30)).then("21살~30살")
                        .otherwise("기타"))
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    //상수 넣기
    @Test
    public void constant() {
        List<Tuple> result = query
                //가져온 회원 이름 옆에 상수 문자열 "A"가 들어감
                .select(member.userName, Expressions.constant("A"))
                .from(member)
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    @Test
    public void concat() {
        List<String> result = query
                //.stringValue()함수는 ENUM을 처리할 때 많이 사용된다
                .select(member.userName.concat("_").concat(member.age.stringValue()))
                .from(member)
                .where(member.userName.eq("member1"))
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    //프로젝션 대상이 하나 -> member.userName
    @Test
    public void simpleProjection() {
        List<String> result = query
                .select(member.userName)
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    //프로젝션 대상이 둘 이상 -> member.userName, member.age
    //Tuple은 되도록이면 레파지토리에서만 쓰고 외부로 즉 서비스나 컨트롤러로 나갈 때
    //DTO등으로 변환해서 내보내는 게 좋다 이유는?
    //의존관계를 가지지 않게 함으로 레파지토리에서 다른 기능으로 바꾸어도 서비스 혹은 컨트롤러에 영향을 미치지 않음
    @Test
    public void tupleProjection() {
        List<Tuple> result = query
                .select(member.userName, member.age)
                .from(member)
                .fetch();

        for (Tuple tuple : result) {
//            String username = tuple.get(member.userName);
//            Integer age = tuple.get(member.age);
//            System.out.println("username = " + username);
//            System.out.println("age = " + age);
            System.out.println("tuple = " + tuple);
        }
    }

    //JPQL로 DTO 조회방법
    @Test
    public void findDtoByJpql() {
//        em.createQuery("select m from Member m",Member.class) -> Entity를 조회
        List<MemberDto> result = em.createQuery("select new study.querydsl.dto.MemberDto(m.userName, m.age) from Member m", MemberDto.class)
                .getResultList();
//        new ~ MemberDto(m.userName, m.age) <- MemberDto 클래스 안에 정의한 생성자에 맞게

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    //Getter, Setter
    @Test
    public void findDtoBySetter() {
        List<MemberDto> result = query
                .select(Projections.bean(MemberDto.class,
                        member.userName,
                        member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    //field에 값 반영
    @Test
    public void findDtoByField() {
        List<MemberDto> result = query
                .select(Projections.fields(MemberDto.class,
                        member.userName,
                        member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    //DTO 클래스에 정의해둔 생성자에 타입이 일치해야 함
    @Test
    public void findDtoByConstructor() {
        List<MemberDto> result = query
                .select(Projections.constructor(MemberDto.class,
                        member.userName,
                        member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    //프로퍼티 명이 다를 때 .as()함수를 사용해서 프로퍼티 명을 맞춰주면 된다
    @Test
    public void findUserDto() {
        QMember memberSub = new QMember("memberSub");
        List<UserDto> result = query
                .select(Projections.fields(UserDto.class,
                        member.userName.as("name"),

                                ExpressionUtils.as(JPAExpressions
                                                .select(memberSub.age.max())
                                        .from(memberSub), "age")
                ))
                .from(member)
                .fetch();

        for (UserDto userDto : result) {
            System.out.println("userDto = " + userDto);
        }
    }

    @Test
    public void findUserDtoByConstructor() {
        List<UserDto> result = query
                .select(Projections.constructor(UserDto.class,
                        member.userName,
                        member.age))
                .from(member)
                .fetch();

        for (UserDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

}
