package homework_2023_12_20.classes;

import com.github.javafaker.Faker;
import homework_2023_12_20.exception.*;

import java.util.*;

public class Tournament {
    private static final Faker FAKER = new Faker();

    public static <T extends Participant> void teamsGenerate(int countTeam, int countParticipant, Class<T> type) throws WrongClassCreateTeamException {

        for (int i = 0; i < countTeam; i++) {
            Team<T> team = new Team<>(FAKER.team().name());
            team.generateTeam(countParticipant, type);
            Handler.addTeam(team);
        }
    }

    public static <T extends Participant> void makeTournament(Set<Team<? extends Participant>> allTeams, Class<T> type) {
        makeGroups(Handler.getTeams(), type);

        for (Team<? extends Participant> team : allTeams) {
            Handler.addTeamResult(team);
        }
    }

    public static void resultTournament() {
        sortedMap();
        for (Team<? extends Participant> team : Handler.getTableAllGroups().keySet()) {
            System.out.println("Название команды - " + team.getName() + " | Очки команды: " + team.getScore() + " | Группа: " + team.getParticipants().get(0).getClass().getSimpleName());
        }
    }

    public static <T extends Participant> void resultTournamentByGroups(Class<T> type) {
        List<Team<T>> group = resultGroup(type);

        group.sort(Comparator.comparing(Team::getScore, Comparator.reverseOrder()));

        System.out.println("Результаты группы " + group.get(0).getParticipants().get(0).getClass().getSimpleName() + ":");
        for (Team<T> team : group) {
            System.out.println("Название команды - " + team.getName() + " | Очки команды: " + team.getScore());
        }
        System.out.println();
    }

    private static <T extends Participant> List<Team<T>> resultGroup(Class<T> type) {
        List<Team<T>> group = new ArrayList<>();
        for (Team<? extends Participant> team : Handler.getTableAllGroups().keySet()) {
            if (team.getParticipants().get(0).getClass().isAssignableFrom(type)) {
                group.add((Team<T>) team);
            }
        }

        return group;
    }

    private static <T extends Participant> void makeGroups(Set<Team<? extends Participant>> allTeams, Class<T> type) {
        List<Team<T>> group = new ArrayList<>();

        for (Team<? extends Participant> team : allTeams) {
            if (team.getParticipants().get(0).getClass().isAssignableFrom(type)) {
                group.add((Team<T>) team);
            }
        }
        playGroup(group);
        checkGroup(group);
        group.sort(Comparator.comparing(Team::getScore, Comparator.reverseOrder()));
    }

    private static <T extends Participant> void checkGroup(List<Team<T>> group) {
        group.sort(Comparator.comparing(Team::getScore, Comparator.reverseOrder()));


        List<Team<T>> leaders = new ArrayList<>();

        if (group.size() == 2) {
            if (group.get(0).getScore() == group.get(1).getScore()) {
                leaders.addAll(group);
            }
        } else if (group.size() < 5) {
            if (group.get(0).getScore() == group.get(1).getScore() || group.get(1).getScore() == group.get(2).getScore()) {
                leaders.addAll(group);
            }
        } else {
            if (group.get(0).getScore() == group.get(1).getScore() || group.get(1).getScore() == group.get(2).getScore()) {
                for (int i = 0; i < 5; i++) {
                    leaders.add(group.get(i));
                }
            }
        }

        checkLeaders(leaders);
    }

    private static <T extends Participant> void checkLeaders(List<Team<T>> leaders) {

        if (leaders.size() > 1 && helperChecked(leaders)) {
            playGroup(leaders);
            checkLeaders(leaders);
        }
    }

    private static <T extends Participant> boolean helperChecked(List<Team<T>> leaders) {
        leaders.sort(Comparator.comparing(Team::getScore, Comparator.reverseOrder()));
        if (leaders.size() == 2) {
            return leaders.get(0).getScore() == leaders.get(1).getScore();
        } else {
            for (int i = 0; i < 2; i++) {
                if (leaders.get(i).getScore() == leaders.get(i + 1).getScore()) {
                    return true;
                }
            }
        }
        return false;
    }

    private static <T extends Participant> void playGroup(List<Team<T>> teams) throws EmptyTableGroupException {
        if (teams.size() <= 1) {
            throw new EnoughPlayersTeamException(ErrorMessage.ENOUGH_PLAYERS_TEAM);
        } else {
            for (int i = 0; i < teams.size() - 1; i++) {
                for (int j = i + 1; j < teams.size(); j++) {
                    teams.get(i).play(teams.get(j));
                }
            }
        }
    }

    private static void sortedMap() {
        Map<Team<? extends Participant>, Double> sortedMap = new LinkedHashMap<>();

        List<Map.Entry<Team<? extends Participant>, Double>> sortedList = new ArrayList<>(Handler.getTableAllGroups().entrySet());
        sortedList.sort(Comparator.comparing(Map.Entry::getValue, Comparator.reverseOrder()));

        for (Map.Entry<Team<? extends Participant>, Double> entry : sortedList) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        Handler.setTableAllGroups(sortedMap);
    }

    public static void highScoringTeam() throws EmptyTableGroupException {

        if (Handler.getTableAllGroups().isEmpty()) {
            throw new EmptyTableGroupException(ErrorMessage.EMPTY_TABLE_GROUP);
        } else {
            Team<? extends Participant> team = null;

            for (Team<? extends Participant> team1 : Handler.getTableAllGroups().keySet()) {
                if (team == null) {
                    team = team1;
                }
                if (team1.getScore() > team.getScore()) {
                    team = team1;
                }
            }
            System.out.println("Команда с максимальным баллом:");
            System.out.println(team.getName() + " | Очки команды - " + team.getScore() + " | Группа: " + team.getParticipants().get(0).getClass().getSimpleName());
        }
    }

    public static void allScores() throws EmptyListTeamsExceptions {
        if (Handler.getTeams().isEmpty()) {
            throw new EmptyListTeamsExceptions(ErrorMessage.EMPTY_LIST_TEAMS);
        } else {
            double allScores = 0;
            for (Team<? extends Participant> team : Handler.getTeams()) {
                allScores += team.getScore();
            }

            System.out.println("Общее количество баллов - " + allScores);
        }
    }

    public static void ListWithoutScore() throws EmptyListTeamsExceptions {

        if (Handler.getTeams().isEmpty()) {
            throw new EmptyListTeamsExceptions(ErrorMessage.EMPTY_LIST_TEAMS);
        } else {
            System.out.println("Команды без баллов:");
            for (Team<? extends Participant> team : Handler.getTeams()) {
                if (team.getScore() == 0) {
                    System.out.println(team.getName());
                }
            }
        }
    }

    public static void averageAgeTeams() throws EmptyListTeamsExceptions {
        if (Handler.getTeams().isEmpty()) {
            throw new EmptyListTeamsExceptions(ErrorMessage.EMPTY_LIST_TEAMS);
        } else {
            for (Team<? extends Participant> team : Handler.getTeams()) {
                System.out.print("Название команды " + team.getName());
                double agesTeams = 0;
                for (Participant participant : team.getParticipants()) {
                    agesTeams += participant.getAge();
                }
                System.out.println(" | Средний возраст команды - " + agesTeams / team.getParticipants().size());
            }
        }
    }

    public static void teamHigherAverage() throws EmptyListTeamsExceptions {
        if (Handler.getTeams().isEmpty()) {
            throw new EmptyListTeamsExceptions(ErrorMessage.EMPTY_LIST_TEAMS);
        } else {
            System.out.println("Команды выше среднего результата - " + averageScoreTeams());
            for (Team<? extends Participant> team : Handler.getTeams()) {
                if (team.getScore() > averageScoreTeams()) {
                    System.out.println("Название команды: " + team.getName() + " | Очки команды - " + team.getScore());
                }
            }
        }
    }

    private static double averageScoreTeams() {
        double allScore = 0;
        for (Team<? extends Participant> team : Handler.getTeams()) {
            allScore += team.getScore();
        }

        return allScore / Handler.getTeams().size();
    }

    public static <T extends Participant> void searchByClass(Class<T> type) throws EmptyListTeamsExceptions {
        if (Handler.getTeams().isEmpty()) {
            throw new EmptyListTeamsExceptions(ErrorMessage.EMPTY_LIST_TEAMS);
        } else {
            for (Team<? extends Participant> team : Handler.getTeams()) {
                if (team.getParticipants().get(0).getClass().isAssignableFrom(type)) {
                    System.out.println("Название команды - " + team.getName());
                }
            }
        }
    }

    public static <T extends Participant> void findingWinnersOverTeam(Team<T> team) throws EmptyHistoryMatchesException {
        if (Handler.getHistoryMatches().isEmpty()) {
            throw new EmptyHistoryMatchesException(ErrorMessage.EMPTY_HISTORY_MATCHES);
        } else {
            for (Map.Entry<List<Team<? extends Participant>>, List<Double>> match : Handler.getHistoryMatches().entrySet()) {
                if (match.getKey().get(0).equals(team) && match.getValue().contains(0D)) {

                    System.out.println("Данная команда: " + match.getKey().get(1).getName() + " | Выграла - " + match.getKey().get(0).getName());
                }
                if (match.getKey().get(1).equals(team) && match.getValue().contains(1D)) {
                    System.out.println("Данная команда: " + match.getKey().get(0).getName() + " | Выграла - " + match.getKey().get(1).getName());
                }
            }
        }
    }

    public static void findingYoungestPlayer() throws EmptyListTeamsExceptions {
        if (Handler.getTeams().isEmpty()) {
            throw new EmptyListTeamsExceptions(ErrorMessage.EMPTY_LIST_TEAMS);
        } else {
            Participant youngestPlayer = null;

            for (Team<? extends Participant> team : Handler.getTeams()) {
                for (Participant participant : team.getParticipants()) {
                    if (youngestPlayer == null || youngestPlayer.getAge() > participant.getAge()) {
                        youngestPlayer = participant;
                    }
                }
            }

            System.out.println("Самы молодой игрок - " + youngestPlayer.getName());
        }
    }

    public static <T extends Participant> void findingExperiencedTeam() throws EmptyListTeamsExceptions {
        if (Handler.getTeams().isEmpty()) {
            throw new EmptyListTeamsExceptions(ErrorMessage.EMPTY_LIST_TEAMS);
        } else {
            Team<T> experiencedTeam = null;
            int teamAge = 0;

            for (Team<? extends Participant> team : Handler.getTeams()) {
                int countAge = 0;
                for (Participant participant : team.getParticipants()) {
                    countAge += participant.getAge();
                }
                if (experiencedTeam == null || teamAge < countAge) {
                    experiencedTeam = (Team<T>) team;
                    teamAge = countAge;
                }
            }

            System.out.println("Самая опытная команда: " + experiencedTeam.getName() + " | Суммарный возраст - " + teamAge);
        }
    }

    public static void findingByDiapasonAge(int from, int to) throws EmptyListTeamsExceptions {
        if (Handler.getTeams().isEmpty()) {
            throw new EmptyListTeamsExceptions(ErrorMessage.EMPTY_LIST_TEAMS);
        } else {
            for (Team<? extends Participant> team : Handler.getTeams()) {
                if (checkDiapason(team, from, to)) {
                    System.out.println("Название команды: " + team.getName());
                    for (Participant participant : team.getParticipants()) {
                        System.out.println("Игрок: " + participant.getName() + " | Возраст " + participant.getAge());
                    }
                }

            }
        }
    }

    private static boolean checkDiapason(Team<? extends Participant> team, int from, int to) {
        for (Participant participant : team.getParticipants()) {
            if (participant.getAge() < from || participant.getAge() > to) {
                return false;
            }
        }

        return true;
    }

    public static void sortParticipantByAge() throws EmptyListTeamsExceptions {
        if (Handler.getTeams().isEmpty()) {
            throw new EmptyListTeamsExceptions(ErrorMessage.EMPTY_LIST_TEAMS);
        } else {
            ArrayList<Participant> sortByAge = new ArrayList<>();
            for (Team<? extends Participant> team : Handler.getTeams()) {
                for (Participant participant : team.getParticipants()) {
                    sortByAge.add(participant);
                }
            }

            sortByAge.sort(Comparator.comparing(Participant::getAge, Comparator.reverseOrder()));
            int count = 1;
            for (Participant participant : sortByAge) {
                System.out.println(count + ".Имя: " + participant.getName() + " | Возраст - " + participant.getAge());
                count++;
            }
        }
    }

    public static <T extends Participant> void findingBiggestDiapasonByAge() throws EmptyListTeamsExceptions {
        if (Handler.getTeams().isEmpty()) {
            throw new EmptyListTeamsExceptions(ErrorMessage.EMPTY_LIST_TEAMS);
        } else {
            Team<T> team = null;
            int diapason = 0;

            for (Team<? extends Participant> team1 : Handler.getTeams()) {
                if (team == null) {
                    team = (Team<T>) team1;
                }
                int ageMax = Integer.MIN_VALUE;
                int ageMin = Integer.MAX_VALUE;
                for (Participant participant : team1.getParticipants()) {
                    if (participant.getAge() > ageMax) {
                        ageMax = participant.getAge();
                    } else if (participant.getAge() < ageMin) {
                        ageMin = participant.getAge();
                    }
                }
                if (diapason < ageMax - ageMin) {
                    team = (Team<T>) team1;
                    diapason = ageMax - ageMin;
                }
            }

            System.out.println("Самый большая разница в возрасте у команды: " + team.getName() + " | Разница - " + diapason);
        }
    }

    public static void findingAllTeamsWithTotalAge() throws EmptyListTeamsExceptions {
        if (Handler.getTeams().isEmpty()) {
            throw new EmptyListTeamsExceptions(ErrorMessage.EMPTY_LIST_TEAMS);
        } else {
            Set<Team<? extends Participant>> clone = new HashSet<>(Handler.getTeams());

            for (Team<? extends Participant> team : Handler.getTeams()) {
                Set<Team<? extends Participant>> teamsWithTotalAge = new HashSet<>();
                teamsWithTotalAge.add(team);
                int totalAge = totalAgeTeam(team);

                for (Team<? extends Participant> teamNext : clone) {
                    if (team.equals(teamNext)) {
                        continue;
                    }
                    if (totalAgeTeam(teamNext) == totalAge) {
                        teamsWithTotalAge.add(teamNext);
                    }
                }
                clone.removeAll(teamsWithTotalAge);
                if (teamsWithTotalAge.size() > 1) {
                    System.out.println("Суммарный возраст - " + totalAge);
                    for (Team<? extends Participant> teamSet : teamsWithTotalAge) {
                        System.out.println("Название команды: " + teamSet.getName() + " | Суммарный возраст - " + totalAge);
                    }
                    System.out.println();

                }
            }
        }
    }

    private static int totalAgeTeam(Team<? extends Participant> team) {
        int totalAge = 0;
        for (Participant participant : team.getParticipants()) {
            totalAge += participant.getAge();
        }
        return totalAge;
    }

    public static <T extends Participant> void averageScoreGroup(Class<T> type) throws EmptyListTeamsExceptions {
        if (Handler.getTeams().isEmpty()) {
            throw new EmptyListTeamsExceptions(ErrorMessage.EMPTY_LIST_TEAMS);
        } else {
            int countTeams = 0;
            double scoreGroup = 0;

            for (Team<? extends Participant> team : Handler.getTeams()) {
                if (team.getParticipants().get(0).getClass().isAssignableFrom(type)) {
                    countTeams++;
                    scoreGroup += team.getScore();
                }
            }

            System.out.println();
        }
    }

    public static void teamScoreBeBetter() throws EmptyListTeamsExceptions {
        if (Handler.getTeams().isEmpty()) {
            throw new EmptyListTeamsExceptions(ErrorMessage.EMPTY_LIST_TEAMS);
        } else {
            for (Team<? extends Participant> team : Handler.getTeams()) {
                if (checkTeamBetter(team)) {
                    System.out.println("У команды всегда улучшались результаты - " + team.getName());
                }
            }
        }
    }

    private static boolean checkTeamBetter(Team<? extends Participant> team) {

        for (Map.Entry<List<Team<? extends Participant>>, List<Double>> matches : Handler.getHistoryMatches().entrySet()) {
            if (matches.getKey().get(0).equals(team) && matches.getValue().contains(0D)) {
                return false;
            }
            if (matches.getKey().get(1).equals(team) && matches.getValue().contains(1D)) {
                return false;
            }
        }

        return true;
    }

    public static <T extends Participant> void findingTeamsWithDraw(Team<T> team) throws EmptyHistoryMatchesException {
        if (Handler.getHistoryMatches().isEmpty()) {
            throw new EmptyHistoryMatchesException(ErrorMessage.EMPTY_HISTORY_MATCHES);
        } else {
            System.out.println(team.getName() + " Сыграла в ничью с:");
            for (Map.Entry<List<Team<? extends Participant>>, List<Double>> matches : Handler.getHistoryMatches().entrySet()) {
                if (matches.getKey().get(0).equals(team) && matches.getValue().contains(0.5)) {
                    System.out.println("Название команды: " + matches.getKey().get(1).getName());
                }
                if (matches.getKey().get(1).equals(team) && matches.getValue().contains(0.5)) {
                    System.out.println("Название команды: " + matches.getKey().get(0).getName());
                }
            }
        }
    }

    public static <T extends Participant> void resultMatchesBetweenTeams(Team<T> first, Team<T> second) throws EmptyHistoryMatchesException {
        if (Handler.getHistoryMatches().isEmpty()) {
            throw new EmptyHistoryMatchesException(ErrorMessage.EMPTY_HISTORY_MATCHES);
        } else {
            for (Map.Entry<List<Team<? extends Participant>>, List<Double>> matches : Handler.getHistoryMatches().entrySet()) {
                if (matches.getKey().contains(first) && matches.getKey().contains(second)) {
                    for (Double score : matches.getValue()) {
                        if (score == 1) {
                            System.out.println(matches.getKey().get(0).getName() + " - " + matches.getKey().get(1).getName() + " | Счет 1 : 0");
                        } else if (score == 0) {
                            System.out.println(matches.getKey().get(0).getName() + " - " + matches.getKey().get(1).getName() + " | Счет 0 : 1");
                        } else {
                            System.out.println(matches.getKey().get(0).getName() + " - " + matches.getKey().get(1).getName() + " | Счет 0 : 0");
                        }
                    }
                }
            }
        }
    }

    public static void infoAboutTeams(Team<? extends Participant> first, Team<? extends Participant> second) throws EmptyHistoryMatchesException {
        int countMatchesFirst = countMatches(first);
        int countMatchesSecond = countMatches(second);

        double averageAgeFirst = averageAgeTeam(first);
        double averageAgeSecond = averageAgeTeam(second);

        System.out.println("Название первой команды: " + first.getName() + " | Средний балл команды - "
                + first.getScore() / countMatchesFirst + " | Средний возраст команды - " + averageAgeFirst);
        System.out.println();
        System.out.println("Название второй команды: " + second.getName() + " | Средний балл команды - "
                + second.getScore() / countMatchesSecond + " | Средний возраст команды - " + averageAgeSecond);
    }

    private static double averageAgeTeam(Team<? extends Participant> team) {
        double allAge = 0;
        for (Participant participant : team.getParticipants()) {
            allAge += participant.getAge();
        }

        return allAge / team.getParticipants().size();
    }

    public static int countMatches(Team<? extends Participant> team) throws EmptyHistoryMatchesException {
        if (Handler.getHistoryMatches().isEmpty()) {
            throw new EmptyHistoryMatchesException(ErrorMessage.EMPTY_HISTORY_MATCHES);
        } else {
            int count = 0;
            for (Map.Entry<List<Team<? extends Participant>>, List<Double>> matches : Handler.getHistoryMatches().entrySet()) {
                if (matches.getKey().contains(team)) {
                    count += matches.getValue().size();
                }
            }

            return count;
        }
    }

    public static void teamsStreakWins() throws EmptyListTeamsExceptions {
        if (Handler.getTeams().isEmpty()) {
            throw new EmptyListTeamsExceptions(ErrorMessage.EMPTY_LIST_TEAMS);
        } else {
            Team<? extends Participant> team = null;

            for (Team<? extends Participant> team1 : Handler.getTeams()) {
                if (team == null) {
                    team = team1;
                }
                if (team.getMaxStreakWins() < team1.getMaxStreakWins()) {
                    team = team1;
                }
            }

            System.out.println("Название команды: " + team.getName() + " | Серия побед - " + team.getMaxStreakWins());
        }
    }

    public static void teamsStreakDraws() {
        Team<? extends Participant> team = null;
        int allDraws = 0;

        for (Team<? extends Participant> team1 : Handler.getTeams()) {
            if (team == null) {
                team = team1;
            }
            int count = 0;
            for (Map.Entry<List<Team<? extends Participant>>, List<Double>> matches : Handler.getHistoryMatches().entrySet()) {
                if (matches.getKey().contains(team1) && matches.getValue().contains(0.5)) {
                    for (Double score : matches.getValue()) {
                        if (score == 0.5) {
                            count++;
                        }
                    }
                }
            }

            if (allDraws < count) {
                team = team1;
                allDraws = count;
            }
        }

        System.out.println("Название команды: " + team.getName() + " | Количество ничьей - " + allDraws);
    }
}