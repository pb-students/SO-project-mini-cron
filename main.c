#include <stdlib.h>
#include <signal.h>
#include <sys/syslog.h>
#include <sys/stat.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <inttypes.h>
#include <time.h>
#include <errno.h>
#include <limits.h>
#include <wait.h>

// Spis tresci:
// A | Tryby wypisywania
// B | Taski
// C | Wyliczanie indexow sekund
// D | Lista taskow
// E | Sygnaly
// F | Demon sysvinit
// G | Wykonywanie taskow
// H | Przekazywanie potokow
// I | Pobieranie pelnej sciezki
// J | Dzielenie stringa

// A | Definicja trybow wpisywania jako flagi binarne
// A | W zadaniu masz napisane, ze tryb 0 to stdout, 1 to stderr, a 2 to i jeden i drugi.
// A | Jezeli do tych wartosci dodamy 1, to mozemy to uproscic do flag binarnych, gdzie:
// A | stdout - 1 + 0 = 1 - 0b01
// A | stderr - 1 + 1 = 2 - 0b10
#define MODE_STDOUT 1
#define MODE_STDERR 2

// B | Struktura taska
// B | Trzymamy komendy jako stringa
// B | I godziny, minuty oraz tryb
struct task {
    char* commands;
    int hour;
    int minute;
    int mode;
};

char* taskfile = NULL;
char* outfile = NULL;
struct task** tasklist = NULL;

char **split(char *string, char *divider);

char *filepath(char *path);

void graceful_close (int signum) {
    exit(EXIT_SUCCESS);
}

// C | Zamienienie godzin i sekund na same sekundy
int get_seconds (int hour, int minute) {
    return hour * 60 + minute;
}

int get_task_seconds (struct task* task) {
    return get_seconds(task->hour, task->minute);
}

// C | Zamienienie godzin i sekund na same sekundy dla aktualnej godziny
int get_curr_seconds () {
    // Pobieramy aktualna godzine i minute
    time_t now = time(NULL);
    struct tm *tm_struct = localtime(&now);
    return get_seconds(tm_struct->tm_hour, tm_struct->tm_min);
}

// B | Funkcja do ustawiania odpowiednich pol w strukturze taska
// B | Podajesz do niej taska, numer wczytanej z pliku kolumny i string jaki jest wczytany w tej kolumnie
void set_task_field (struct task* task, int column, char* column_str) {
    switch (column) {
        case 0:
            // B | Pierwsza i druga kolumna to inty
            // B | Wiec w bezpieczny sposob zamieniamy je na inty bez znakow (unsigned int)
            task->hour = strtoumax(column_str, NULL, 10);
            free(column_str);

            // B | Jezeli parsowanie sie nie powiodlo - wywalamy blad
            if (task->hour == UINTMAX_MAX && errno == ERANGE) {
                syslog(LOG_ERR, "Cannot open taskfile.");
                perror("strtoumax()");
                exit(EXIT_FAILURE);
            }
            break;
        case 1:
            task->minute = strtoumax(column_str, NULL, 10);
            free(column_str);

            if (task->minute == UINTMAX_MAX && errno == ERANGE) {
                syslog(LOG_ERR, "Cannot open taskfile.");
                perror("strtoumax()");
                exit(EXIT_FAILURE);
            }
            break;

        case 2:
            // B | Trzecia kolumna to polecenie jakie ma sie wykonac
            task->commands = column_str;
            break;

        case 3:
            // B | Czwarta to tryb, zamieniamy na inta i dodajemy 1
            // C | Flagi binarne sa prostsze pozniej w zaimplementowaniu
            // C |
            // C | Zamiast pisac:
            // C | if (task->mode == 0 || task->mode == 2) { }
            // C | if (task->mode == 1 || task->mode == 2) { }
            // C |
            // C | Mozna do trybu dodac 1 i napisac:
            // C | if (task->mode & 1) { }
            // C | if (task->mode & 2) { }
            task->mode = 1 + strtoumax(column_str, NULL, 10);
            free(column_str);

            if (task->mode == UINTMAX_MAX && errno == ERANGE) {
                syslog(LOG_ERR, "Cannot open taskfile.");
                perror("strtoumax()");
                exit(EXIT_FAILURE);
            }
            break;
    }
}

// D | Funkcja do przeladowania listy taskow
void reload_tasks (int signum) {
    syslog(LOG_NOTICE, "Loading taskfile: %s", taskfile);
    int fd = open(taskfile, O_RDONLY);

    if (fd < 0) {
        syslog(LOG_ERR, "Cannot open taskfile.");
        perror("open()");
        exit(EXIT_FAILURE);
    }

    // D | Wczytujemy dlugosc pliku ustawiajac kursor na koniec pliku
    // D | lseek zwraca na ktorym znaku jestesmy
    // D | Wiec pozniej ustawiamy znowu na poczatek
    int size = lseek(fd, 0, SEEK_END);
    lseek(fd, 0, SEEK_SET);

    // D | Wczytujemy odrazu caly plik do buforu
    char buf[size + 1];
    buf[size] = '\0';
    if (read(fd, buf, size) < 0) {
        syslog(LOG_ERR, "Cannot read taskfile.");
        perror("read()");
        exit(EXIT_FAILURE);
    }

    // D | Zamykamy fd na plik
    close(fd);

    // D | Jezeli mamy juz jakas liste taskow, to uwalniamy jej pamiec
    if (tasklist != NULL) {
        free(tasklist);
    }

    // D | Zmienna, ktora mowi o tym ktory z kolei task teraz dodajemy
    int t = 0;
    // D | Alokujemy nowa pamiec na 16 taskow (zmienna tsize)
    // D | W przyszlosci, jak skonczy nam sie miejsce w momencie dodawania taska do listy
    // D | To rozszerzymy sobie liste przez przemnozenie jej wielkosci przez 2
    int tsize = 16;
    tasklist = malloc(tsize * sizeof(struct task*));
    for (int k = 0; k < tsize; ++k) tasklist[k] = NULL;

    // D | Zmienne, ktore pomagaja nam sledzic w ktorej kolumnie jestesmy
    // D | i na jakim indexie zaczyna sie nastepna kolumna
    int column = 0;
    int last_column_idx = 0;

    // D | Tworzymy pustego taska, na ktorym bedziemy ustawiac wartosci
    struct task* task = malloc(sizeof(struct task));
    task->commands = NULL;

    // D | Iterujemy po kazdym znaku w buforze
    for (int i = 0; i < size + 1; ++i) {
        // D | Jezeli trafilismy na znak dzielacy kolumny
        if (buf[i] == ':') {
            // D | Wycinamy aktualna kolumne
            char* column_str = malloc(sizeof(char) * (i - last_column_idx + 1));
            memset(column_str, '\0', i - last_column_idx + 1);
            strncpy(column_str, buf + last_column_idx, i - last_column_idx);

            // D | Ustawiamy pole w tasku
            // D | Podbijamy licznik mowiacy o tym w ktorej kolumnie jestesmy
            set_task_field(task, column++, column_str);

            // D | Ustawiamy, ze nastepna kolumna ma sie zaczac na indexie znaku `:` + 1
            // D | Zmienna nazwana jest `last_...` poniewaz w wiekszosci przypadkow patrze na nia z perspektywy nastepnej kolumny
            last_column_idx = i + 1;
            continue;
        }

        // D | Gdy trafimy na koniec linii lub koniec pliku (by wspierac przypadek, gdy nie mamy entera na koncu)
        // D | Powinnismy wywolac tego ifa tylko na koncu jednego wpisu
        if (buf[i] == '\n' || buf[i] == '\0') {
            // D | Jezeli komenda w aktualnym tasku jest pusta, to znaczy, ze aktualny wpis jest zle sformulowany i za wczesnie skonczony, wiec pomijamy
            if (task->commands == NULL) {
                continue;
            }

            // D | Robimy dokladnie to samo co wczesniej
            char* column_str = malloc(sizeof(char) * (i - last_column_idx + 1));
            memset(column_str, '\0', i - last_column_idx + 1);
            strncpy(column_str, buf + last_column_idx, i - last_column_idx);

            // D | Nie podbijamy kolumny
            set_task_field(task, column, column_str);

            // D | Dodajemy taska do listy taskow
            tasklist[t++] = task;

            // D | Tworzymy nowego taska i resetujemy kolumne
            task = malloc(sizeof(struct task));
            task->commands = NULL;
            column = 0;

            // D | Jezeli nastepny index jest wiekszy niz rozmiar listy to podwajamy liste
            if (t + 1 >= tsize) {
                tasklist = realloc(tasklist, sizeof(struct task) * tsize * 2);
                for (int k = 0; k < tsize; ++k) tasklist[size + k] = NULL;
                tsize *= 2;
            }

            last_column_idx = i + 1;
            continue;
        }
    }

    // D | Sortujemy taski po godzinie wykonania
    // D | Nie wiem jak sie ten algorytm nazywa, ale to ten, ze wstawiasz na indexy, a potem lecisz po indexach
    int task_count[24 * 60];
    struct task** sorted[24 * 60];

    // D | Inicjalizujemy tablice tablic i tablice indexow
    for (int i = 0; i < 24 * 60; ++i) {
        sorted[i] = malloc(sizeof(struct task*) * t);
        task_count[i] = 0;
    }

    // D | Od zera do t; W tym momencie t jest liczba wszystkich taskow
    for (int i = 0; i < t; ++i) {
        // C | Ustawiamy index jako sekundy w podanej godzinie
        int idx = get_task_seconds(tasklist[i]);

        // D | Dodajemy do tablicy na indexie idx taska
        // D | Tablica wyglada np tak:
        // D | Dla godziny 12:37:
        // D | idx = 12 * 60 + 37 = 757
        // D |
        // D | [
        // D |   ...
        // D |   756: [],
        // D |   757: [task],
        // D |   758: [],
        // D |   ...
        // D | ]
        // D |
        sorted[idx][task_count[idx]++] = tasklist[i];
    }

    // D | Pobieramy aktualne sekundy
    int curr_idx = get_curr_seconds();

    // D | Zmienna l jed do sledzenia ile taskow juz dodalismy
    int l = 0;

    // D | Wrzucamy wpierw rzeczy w kolejnosci zaczynajac od tych ktore sa >= aktualnej godzinie
    // D | Wpierw lecimy po godzinach, ktore sa mniejsze od aktualnej
    for (int i = curr_idx; i < 24 * 60; ++i) {
        // D | Potem po wszystkich elementach w tablicy
        for (int j = 0; j < task_count[i]; ++j) {
            tasklist[l++] = sorted[i][j];
        }

        free(sorted[i]);
    }

    // D | Wrzucamy reszte
    for (int i = 0; i < curr_idx; ++i) {
        for (int j = 0; j < task_count[i]; ++j) {
            tasklist[l++] = sorted[i][j];
        }

        free(sorted[i]);
    }
}

// D | Wyswietlanie listy taskow
void jobs (int signum) {
    syslog(LOG_NOTICE, "Current jobs:");

    // D | Dopoki task != NULL
    for (int i = 0; tasklist[i] != NULL; ++i) {
        syslog(LOG_NOTICE, "%02d:%02d | %d | %s", tasklist[i]->hour, tasklist[i]->minute, tasklist[i]->mode - 1, tasklist[i]->commands);
    }
}

int main(int argc, char* argv[]) {
    // E | Ustawienie handlerow na sygnaly
    if (signal(SIGINT, graceful_close) == SIG_ERR || signal(SIGUSR1, reload_tasks) == SIG_ERR || signal(SIGUSR2, jobs) == SIG_ERR) {
        perror("signal()");
        exit(EXIT_FAILURE);
    }

    if (argc < 3) {
        fprintf(stderr, "Usage: %s <taskfile> <outfile>", argv[0]);
        exit(EXIT_FAILURE);
    }

    taskfile = filepath(argv[1]);
    outfile = filepath(argv[2]);
    truncate(outfile, 0);

    // D | Przeladowujemy liste taskow
    reload_tasks(0);

    // F | Tworzymy podproces
    pid_t pid = fork();
    if (pid == -1) {
        syslog(LOG_ERR, "Error with fork");
        perror("fork()");
        exit(EXIT_FAILURE);
    }

    // F | Zabijamy glowny proces
    if (pid > 0) {
        exit(EXIT_SUCCESS);
    }

    // F | Uruchomiamy umask
    umask(0);

    // F | Uruchomiamy setsid
    pid_t sid = setsid();
    if (sid < 0) {
        syslog(LOG_ERR, "Error with setsid");
        perror("setsid()");
        exit(EXIT_FAILURE);
    }

    // F | Zmieniamy katalog demona na /
    if (chdir("/") == -1) {
        syslog(LOG_ERR, "Error with chdir");
        perror("chdir()");
        exit(EXIT_FAILURE);
    }

    // F | Nie zamykam stdin, stdout i stcerr bo inaczej nie dziala
    // F | Dla demonow sysvinit, specyfikacja mowi by je pozamykac

    // G | Do momentu kiedy pierwszy task != NULL
    while (tasklist[0] != NULL) {
        // G | Czekamy roznice sekund miedzy taskiem, a aktualnym czasem
        sleep(get_task_seconds(tasklist[0]) - get_curr_seconds());

        // G | Po SIGUSR1 mozemy wczytac pusty plik lub moze on sie zmienic
        // G | SIGUSR1 przerywa sleepa
        if (tasklist[0] == NULL || get_task_seconds(tasklist[0]) > get_curr_seconds()) {
            continue;
        }

        // G | Pobieramy task
        struct task* task = tasklist[0];

        // G | Usuwamy task z kolejki
        for (int i = 1; tasklist[i - 1] != NULL; ++i) {
            tasklist[i - 1] = tasklist[i];
        }

        // G | Dzielimy komendy po znaku `|` i wyliczamy ich ilosc
        char** commands = split(task->commands, "|");
        int cmd_num = 0;
        for (int i = 0; commands[i] != NULL; ++i) {
            cmd_num++;
        }

        int fd[cmd_num * 2];
        int pids[cmd_num];

        // H | Bazujac na twopipes.c z http://gunpowder.cs.loyola.edu/~jglenn/702/S2005/Examples/dup2.html
        // H | Przerobione na podejscie iteracyjne

        // H | Iterujemy po wszystkich komendach i robimy pipe
        for (int i = 0; i < cmd_num; ++i) {
            pids[i] = -1;
            pipe(fd + i * 2);
        }

        // G | Otwieramy plik outfile i nadajemy mu uprawnienia rw-rw-r--
        int file = open(outfile, O_WRONLY | O_APPEND | O_CREAT, S_IWUSR | S_IRUSR | S_IRGRP | S_IROTH | S_IWGRP);
        if (file < 0) {
            syslog(LOG_ERR, "Error with open");
            perror("open()");
            exit(EXIT_FAILURE);
        }

        // H | Tworzymy proces potomny
        int pid = fork();
        if (pid < 0) {
            syslog(LOG_ERR, "Error with fork");
            perror("fork()");
            exit(EXIT_FAILURE);
        }

        // H | W procesie potomnym
        if (pid == 0) {
            syslog(LOG_NOTICE, "Task `%s` started", task->commands);

            // G | Wypisujemy do pliku outfile podana komende
            char buf[strlen(task->commands) + 12];
            sprintf(buf, "%02d:%02d:%s:%d\n", task->hour, task->minute, task->commands, task->mode);
            write(file, buf, strlen(buf));

            // G | Iterujemy po wszystkich komendach i odpowiednio je pipujemy
            for (int i = 0; i < cmd_num; ++i) {
                // G | Dzielimy komende po spacji
                char** args = split(commands[i], " ");

                // H | Forkujemy proces
                int inner_pid = fork();
                if (inner_pid < 0) {
                    syslog(LOG_ERR, "Error with fork");
                    perror("fork()");
                    exit(EXIT_FAILURE);
                }

                // H | Dla podprocesu komendy
                if (inner_pid == 0) {
                    // A | Sprawdzamy czy jest ustawiona flaga STDERR i duplikujemy deskryptor outfile na stderr
                    if (task->mode & MODE_STDERR) {
                        dup2(file, STDERR_FILENO);
                    }

                    // A | Jezeli jest ostatni
                    if (i == cmd_num - 1) {
                        // A | Sprawdzamy czy jest ustawiona flaga STDOUT i duplikujemy deskryptor outfile na stdout
                        if (task->mode & MODE_STDOUT) {
                            dup2(file, STDOUT_FILENO);
                        }
                    }

                    // H |  Jezeli nie jest ostatni to przekazujemy stdout dalej
                    if (i != cmd_num - 1) {
                        dup2(fd[i * 2 + 1], STDOUT_FILENO);
                    }

                    // H | Jezeli nie jest pierwszy to przekazujemy stdin dalej
                    if (i != 0) {
                        dup2(fd[i * 2 - 2], STDIN_FILENO);
                    }

                    // H | Zamykamy wszystkie fd
                    close(file);
                    for (int j = 0; j < cmd_num; ++j) {
                        close(fd[j * 2]);
                        close(fd[j * 2 + 1]);
                    }

                    // G | Wykonujemy komende
                    if (execvp(args[0], args) < 0) {
                        syslog(LOG_ERR, "Error with exec: `%s`", commands[i]);
                        perror("exec()");
                        exit(EXIT_FAILURE);
                    }

                    exit(EXIT_SUCCESS);
                }

                // H | podajemy pid do procesu rodzica
                pids[i] = inner_pid;
            }

            // H | Zamykamy wszystkie fd
            close(file);
            for (int i = 0; i < cmd_num; ++i) {
                close(fd[i * 2]);
                close(fd[i * 2 + 1]);
            }

            // H | Czekamy na zakonczenie wszystkich podprocesow odpowiednich programow
            for (int i = 0; i < cmd_num; ++i) {
                int status = -2;
                do {
                    if (waitpid(pids[i], &status, WUNTRACED) == -1) {
                        syslog(LOG_ERR, "Cannot wait for pid %d", pids[i]);
                    }
                } while (!WIFEXITED(status) && !WIFSIGNALED(status));

                if (status != 0) {
                    exit(WEXITSTATUS(status));
                }
            }

            exit(EXIT_SUCCESS);
        }

        // H | W procesie glownym zamykamy wszystkie fd
        for (int i = 0; i < cmd_num; ++i) {
            close(fd[i * 2]);
            close(fd[i * 2 + 1]);
        }

        // G | Czekamy na zakonczenie podprocesu wykonujacego taska
        int status = -2;
        do {
            if (waitpid(pid, &status, WUNTRACED) == -1) {
                syslog(LOG_ERR, "Cannot wait for pid %d", pid);
            }
        } while (!WIFEXITED(status) && !WIFSIGNALED(status));

        write(file, "\n\n", 2);
        close(file);

        syslog(LOG_NOTICE, "Task `%s` finished with status %d", task->commands, WEXITSTATUS(status));
    }
}

// I | Pobieranie pelnej sciezki pliku
char *filepath(char *path) {
    // I | Pobieranie aktualnej sciezki
    char cwd[PATH_MAX];
    getcwd(cwd, sizeof(cwd));

    // I | Sklejanie nowej sciezki
    char* final_path = malloc(sizeof(char) * (strlen(cwd) + strlen(path) + 2));
    strcat(final_path, cwd);
    strcat(final_path, "/");
    strcat(final_path, path);
    strcat(final_path, "\0");
    return final_path;
}

// J | Dzielenie stringa na znaku
char** split(char* original_string, char* divider) {
    // J | strtok niszczy podany do niego string, wiec kopiujemy go do nowego miejsca w pamieci (zmienna string)
    char* string = malloc(sizeof(char) * strlen(original_string));
    memset(string, '\0', strlen(original_string));
    strcpy(string, original_string);

    // J | Tworzymy dynamiczna tablice elementow
    int size = 16;
    int idx = 0;
    char** tokens = malloc(sizeof(char*) * size);
    for (int i = 0; i < size; ++i) tokens[i] = NULL;

    // J | Dzielimy string na odpowiednim znaku
    char* token = strtok(string, divider);
    while (token != NULL) {
        tokens[idx++] = token;

        // J | Rozszerzamy tablice
        if (idx + 1 >= size) {
            tokens = realloc(tokens, sizeof(char*) * size * 2);
            for (int i = 0; i < size; ++i) tokens[size + i] = NULL;
            size *= 2;
        }

        // J | Dzielimy dalej tym znakiem, strtok tak dziwnie dziala, ze sie potem podaje NULL zamiast zmiennej
        token = strtok(NULL, divider);
    }

    return tokens;
}
