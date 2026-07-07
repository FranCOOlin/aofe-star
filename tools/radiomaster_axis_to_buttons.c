#include <errno.h>
#include <fcntl.h>
#include <linux/joystick.h>
#include <linux/uinput.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define DEFAULT_INPUT "/dev/input/by-id/usb-NATIONS_RADIOMASTER_SIM_N32G45x-joystick"
#define HIGH_THRESHOLD 16000
#define LOW_THRESHOLD (-16000)

static int uinput_fd = -1;

static const int virtual_axes[4] = {ABS_X, ABS_Y, ABS_Z, ABS_RX};
static const int virtual_buttons[8] = {
    BTN_A, BTN_B, BTN_X, BTN_Y, BTN_TL, BTN_TR, BTN_SELECT, BTN_START,
};

struct axis_button_map {
    int axis;
    int low_button;
    int high_button;
};

/*
 * Match launch/sys_coop_lift_test_001.launch defaults:
 *   bit 1: SD land
 *   bit 2: hook
 *   bit 3: SA takeoff advance
 *   bit 6: SC single action button
 *
 * A value of -1 means that side of the switch is treated as "button released".
 */
static const struct axis_button_map aux_maps[] = {
    {4, -1, 1},
    {5, -1, 3},
    {6, -1, 2},
    {7, -1, 6},
};

static void emit_event(int fd, unsigned short type, unsigned short code, int value)
{
    struct input_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.type = type;
    ev.code = code;
    ev.value = value;
    if (write(fd, &ev, sizeof(ev)) < 0) {
        perror("write input_event");
        exit(1);
    }
}

static void sync_events(int fd)
{
    emit_event(fd, EV_SYN, SYN_REPORT, 0);
}

static void setup_abs(int fd, unsigned code)
{
    if (ioctl(fd, UI_SET_ABSBIT, code) < 0) {
        perror("UI_SET_ABSBIT");
        exit(1);
    }

    struct uinput_abs_setup abs_setup;
    memset(&abs_setup, 0, sizeof(abs_setup));
    abs_setup.code = code;
    abs_setup.absinfo.minimum = -32767;
    abs_setup.absinfo.maximum = 32767;
    abs_setup.absinfo.flat = 128;

    if (ioctl(fd, UI_ABS_SETUP, &abs_setup) < 0) {
        perror("UI_ABS_SETUP");
        exit(1);
    }
}

static void destroy_device(void)
{
    if (uinput_fd >= 0) {
        ioctl(uinput_fd, UI_DEV_DESTROY);
        close(uinput_fd);
        uinput_fd = -1;
    }
}

static void signal_handler(int signum)
{
    (void)signum;
    destroy_device();
    _exit(0);
}

static int create_virtual_joystick(void)
{
    int fd = open("/dev/uinput", O_WRONLY | O_NONBLOCK);
    if (fd < 0) {
        perror("open /dev/uinput");
        return -1;
    }

    if (ioctl(fd, UI_SET_EVBIT, EV_KEY) < 0) {
        perror("UI_SET_EVBIT EV_KEY");
        close(fd);
        return -1;
    }
    for (size_t i = 0; i < sizeof(virtual_buttons) / sizeof(virtual_buttons[0]); ++i) {
        if (ioctl(fd, UI_SET_KEYBIT, virtual_buttons[i]) < 0) {
            perror("UI_SET_KEYBIT");
            close(fd);
            return -1;
        }
    }

    if (ioctl(fd, UI_SET_EVBIT, EV_ABS) < 0) {
        perror("UI_SET_EVBIT EV_ABS");
        close(fd);
        return -1;
    }
    for (size_t i = 0; i < sizeof(virtual_axes) / sizeof(virtual_axes[0]); ++i) {
        setup_abs(fd, virtual_axes[i]);
    }

    struct uinput_setup setup;
    memset(&setup, 0, sizeof(setup));
    snprintf(setup.name, UINPUT_MAX_NAME_SIZE, "RadioMaster Axis Buttons");
    setup.id.bustype = BUS_USB;
    setup.id.vendor = 0x03eb;
    setup.id.product = 0x6200;
    setup.id.version = 1;

    if (ioctl(fd, UI_DEV_SETUP, &setup) < 0) {
        perror("UI_DEV_SETUP");
        close(fd);
        return -1;
    }

    if (ioctl(fd, UI_DEV_CREATE) < 0) {
        perror("UI_DEV_CREATE");
        close(fd);
        return -1;
    }

    return fd;
}

static void set_button(int fd, int button, int value, int button_state[8])
{
    if (button < 0 || button >= 8) {
        return;
    }

    if (button_state[button] != value) {
        button_state[button] = value;
        emit_event(fd, EV_KEY, virtual_buttons[button], value);
    }
}

static void update_aux_buttons(int fd, int aux_axis, int value, int button_state[8])
{
    const struct axis_button_map *map = NULL;

    for (size_t i = 0; i < sizeof(aux_maps) / sizeof(aux_maps[0]); ++i) {
        if (aux_maps[i].axis == aux_axis) {
            map = &aux_maps[i];
            break;
        }
    }
    if (map == NULL) {
        return;
    }

    int low_active = value < LOW_THRESHOLD ? 1 : 0;
    int high_active = value > HIGH_THRESHOLD ? 1 : 0;

    set_button(fd, map->low_button, low_active, button_state);
    set_button(fd, map->high_button, high_active, button_state);
}

int main(int argc, char **argv)
{
    const char *input_path = argc > 1 ? argv[1] : DEFAULT_INPUT;
    int input_fd = open(input_path, O_RDONLY);
    if (input_fd < 0) {
        fprintf(stderr, "open %s: %s\n", input_path, strerror(errno));
        return 1;
    }

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    atexit(destroy_device);

    uinput_fd = create_virtual_joystick();
    if (uinput_fd < 0) {
        close(input_fd);
        return 1;
    }

    int button_state[8] = {0};
    printf("Reading %s\n", input_path);
    printf("Created virtual joystick: RadioMaster Axis Buttons\n");
    printf("Axis 0-3 pass through.\n");
    printf("Axis 4 high -> button 1, axis 5 high -> button 3, axis 6 high -> button 2.\n");
    printf("Axis 7 high -> button 6 for the SC switch; other SC positions release it.\n");
    fflush(stdout);

    while (true) {
        struct js_event ev;
        ssize_t n = read(input_fd, &ev, sizeof(ev));
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("read joystick");
            return 1;
        }
        if (n != sizeof(ev)) {
            continue;
        }

        unsigned char type = ev.type & ~JS_EVENT_INIT;
        if (type != JS_EVENT_AXIS) {
            continue;
        }

        if (ev.number < 4) {
            emit_event(uinput_fd, EV_ABS, virtual_axes[ev.number], ev.value);
            sync_events(uinput_fd);
        } else if (ev.number < 8) {
            update_aux_buttons(uinput_fd, ev.number, ev.value, button_state);
            sync_events(uinput_fd);
        }
    }
}
