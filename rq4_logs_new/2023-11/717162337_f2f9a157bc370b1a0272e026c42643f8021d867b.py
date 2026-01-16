import numpy
import matplotlib.pyplot as plt

plt.rcParams.update({"font.size": 16})
# plt.rcParams.update({'font.family': 'calibri'})
plt.rcParams.update({"lines.linewidth": 4})
plt.style.use("dark_background")
import savingfigR as sf
import seaborn as sns
from tqdm import tqdm

INITIAL_PORTFOLIO_AMOUNT = 460000
ADVISOR_RATE = 1.24
ADVISOR_PERCENT = ADVISOR_RATE / 100
# N_YEARS = 2
N_YEARS = 35
N_MONTHS = N_YEARS * 12
YEARLY_GROWTH_RATE = 7
MONTHLY_GROWTH_RATE = YEARLY_GROWTH_RATE / 12
MONTHLY_GROWTH_PERCENT = MONTHLY_GROWTH_RATE / 100


def convertToThousands(value):
    return value / 1000


def calculate_monthly_fees(portfolio_amount, advisor_percent):
    one_year_fees = portfolio_amount * advisor_percent
    return one_year_fees / 12


one_month_fees = calculate_monthly_fees(
    portfolio_amount=INITIAL_PORTFOLIO_AMOUNT, advisor_percent=ADVISOR_PERCENT
)
print(f"One month worth of fees = {one_month_fees:.2f}\n")

fees_per_month_rate = []
for i_month in range(N_MONTHS):
    is_first_month = i_month == 0
    if is_first_month:
        portfolio_amount = INITIAL_PORTFOLIO_AMOUNT
    else:
        portfolio_amount = portfolio_amount + (
            portfolio_amount * MONTHLY_GROWTH_PERCENT
        )
    i_month_fee = calculate_monthly_fees(
        portfolio_amount=portfolio_amount, advisor_percent=ADVISOR_PERCENT
    )
    fees_per_month_rate.append(i_month_fee)

cum_fees_per_month_rate = numpy.cumsum(fees_per_month_rate)

fees_per_month_flat = numpy.repeat(100, N_MONTHS)
cum_fees_per_month_flat = numpy.cumsum(fees_per_month_flat)

fnames = []
for i_month in tqdm(range(N_MONTHS)):
    fig, ax = plt.subplots(nrows=1, ncols=1)

    rate_basedH = plt.plot(
        range(0, i_month),
        convertToThousands(cum_fees_per_month_rate[0:i_month]),
        label="Rate-Based",
    )
    flat_feeH = plt.plot(
        range(0, i_month),
        convertToThousands(cum_fees_per_month_flat[0:i_month]),
        label="Flat-Fee",
    )

    plt.xticks(numpy.arange(start=0, stop=450, step=12 * 5))
    # ax.set_xticklabels(numpy.arange(start=0, stop=37, step=5))
    ax.set_xticklabels(numpy.arange(start=0, stop=N_YEARS + 2, step=5))

    ax.yaxis.set_major_formatter("${x:.0f}k")

    plt.xlabel("Years")
    plt.ylabel("Cumulative Fees ($)")
    plt.xlim([0, 450])
    plt.ylim([0, convertToThousands(875000)])
    plt.grid(alpha=0.4)
    plt.tight_layout()
    plt.text(10, convertToThousands(800000), "Rate-Based", color=rate_basedH[0].get_color())
    plt.text(10, convertToThousands(750000), "Flat-Fee", color=flat_feeH[0].get_color())
    # plt.legend(facecolor=colors.RGB_TAN)

    i_fname = f"{i_month}fig.png"
    fnames.append(i_fname)
    # plt.show()
    sf.best_save(fig, i_fname)

sf.writeGif("cumulative_fees", fnames, fps=45)

fig, ax = plt.subplots(nrows=1, ncols=1)

rate_basedH = plt.plot(
    range(N_MONTHS), convertToThousands(cum_fees_per_month_rate), label="Rate-Based"
)
flat_feeH = plt.plot(
    range(N_MONTHS), convertToThousands(cum_fees_per_month_flat), label="Flat-Fee"
)

plt.xticks(numpy.arange(start=0, stop=450, step=12 * 5))
# ax.set_xticklabels(numpy.arange(start=0, stop=37, step=5))
ax.set_xticklabels(numpy.arange(start=0, stop=N_YEARS + 2, step=5))
ax.yaxis.set_major_formatter("${x:.0f}k")

plt.xlabel("Time (Years)")
plt.ylabel("Cumulative Fees ($)")
plt.xlim([0, 450])
plt.grid(alpha=0.4)
plt.tight_layout()
plt.text(10, convertToThousands(800000), "Rate-Based", color=rate_basedH[0].get_color())
plt.text(10, convertToThousands(750000), "Flat-Fee", color=flat_feeH[0].get_color())
# plt.legend(facecolor=colors.RGB_TAN)
# plt.show()
sf.best_save(fig, "cumulative_fees")