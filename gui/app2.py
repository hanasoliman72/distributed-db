import tkinter as tk

# ─────────────────────────────────────────────
#  THEME
# ─────────────────────────────────────────────
BG_DEEP = "#0f1226"
BG_CARD = "#171a33"
WHITE = "#ffffff"
VIOLET = "#7c5cff"
TEAL = "#1ecad3"
GOLD = "#f5c451"
CREAM = "#f7f7fb"

FONT_BTN = ("Segoe UI", 10, "bold")

# ─────────────────────────────────────────────
#  SAFE BUTTON (NO CANVAS)
# ─────────────────────────────────────────────
class FancyButton(tk.Frame):
    def __init__(self, parent, text, command=None,
                 bg=VIOLET, fg=WHITE, width=180, height=38):

        super().__init__(parent, bg=bg, width=width, height=height)

        self.command = command
        self.default_bg = bg
        self.hover_bg = self._lighten(bg, 25)

        self.pack_propagate(False)  # keep fixed size

        self.label = tk.Label(self,
                              text=text,
                              bg=bg,
                              fg=fg,
                              font=FONT_BTN)

        self.label.pack(expand=True, fill="both")

        # Bind events to BOTH frame + label
        for widget in (self, self.label):
            widget.bind("<Enter>", self.on_enter)
            widget.bind("<Leave>", self.on_leave)
            widget.bind("<Button-1>", self.on_click)

    def _lighten(self, hex_col, amt):
        hex_col = hex_col.lstrip("#")
        r, g, b = int(hex_col[0:2],16), int(hex_col[2:4],16), int(hex_col[4:6],16)
        r = min(255, r+amt)
        g = min(255, g+amt)
        b = min(255, b+amt)
        return f"#{r:02x}{g:02x}{b:02x}"

    def on_enter(self, e):
        self.config(bg=self.hover_bg)
        self.label.config(bg=self.hover_bg, cursor="hand2")

    def on_leave(self, e):
        self.config(bg=self.default_bg)
        self.label.config(bg=self.default_bg, cursor="")

    def on_click(self, e):
        if self.command:
            self.command()


# ─────────────────────────────────────────────
#  MAIN APP
# ─────────────────────────────────────────────
class App(tk.Tk):
    def __init__(self):
        super().__init__()

        self.title("Distributed DB Dashboard")
        self.geometry("600x400")
        self.configure(bg=BG_DEEP)

        self._build_ui()

    def _build_ui(self):
        self._card("Write Operations", [
            ("INSERT", self.insert_action, VIOLET),
            ("UPDATE", self.update_action, TEAL),
            ("DELETE", self.delete_action, GOLD)
        ])

    def _card(self, title, buttons):
        frame = tk.Frame(self, bg=BG_CARD)
        frame.pack(padx=20, pady=20, fill="x")

        tk.Label(frame, text=title, fg=WHITE, bg=BG_CARD,
                 font=("Segoe UI", 12, "bold")).pack(anchor="w", pady=(10, 5))

        btn_row = tk.Frame(frame, bg=BG_CARD)
        btn_row.pack(pady=10)

        for btn_text, cmd, color in buttons:
            btn = FancyButton(
                btn_row,
                btn_text,
                command=cmd,
                bg=color,
                fg=BG_DEEP if color in (GOLD, TEAL, CREAM) else WHITE,
                width=180,
                height=38
            )
            btn.pack(side="left", padx=(0, 10))

    # ─────────────────────────
    #  ACTIONS
    # ─────────────────────────
    def insert_action(self):
        print("INSERT clicked")

    def update_action(self):
        print("UPDATE clicked")

    def delete_action(self):
        print("DELETE clicked")


# ─────────────────────────────────────────────
#  RUN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    app = App()
    app.mainloop()